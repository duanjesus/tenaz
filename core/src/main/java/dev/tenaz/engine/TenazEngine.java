package dev.tenaz.engine;

import dev.tenaz.api.JacksonCodec;
import dev.tenaz.api.NonRetryableException;
import dev.tenaz.api.PayloadCodec;
import dev.tenaz.api.StepContext;
import dev.tenaz.api.StepTimeoutException;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowCancelledException;
import dev.tenaz.api.WorkflowFailedException;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.EngineObserver.StepOutcome;
import dev.tenaz.engine.Replay.ChildCancellation;
import dev.tenaz.engine.Replay.Outcome;
import dev.tenaz.engine.Replay.PendingChild;
import dev.tenaz.engine.Replay.PendingStep;
import dev.tenaz.journal.Event;
import dev.tenaz.journal.Journal;
import dev.tenaz.journal.Journal.Delivery;
import dev.tenaz.journal.Journal.FencedException;
import dev.tenaz.journal.Journal.History;
import dev.tenaz.journal.Journal.Lease;
import dev.tenaz.journal.Journal.VersionConflictException;
import dev.tenaz.journal.Journal.WorkflowStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A node of the durable execution engine. Any number of engines may share one {@link Journal};
 * each workflow is driven by whichever engine holds its lease, and is picked up by another one
 * when that engine dies.
 *
 * <p>The engine is a set of short, non-blocking tasks run by an {@link EngineRuntime}, which is
 * what lets the same code run on threads in production and on a seeded event loop in simulation.
 *
 * <p>An engine whose workers were never started is a client: it can start, signal and await
 * workflows that other engines run.
 */
public final class TenazEngine implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(TenazEngine.class.getName());
    private static final int CLAIM_BATCH = 64;
    private static final int PURGE_BATCH = 500;
    private static final String WORKFLOW_ID_IN_USE = "dev.tenaz.WorkflowIdInUse";
    private static final Comparator<Lease> BY_WORKFLOW =
            Comparator.comparing(Lease::workflowId).thenComparingLong(Lease::epoch);

    private final Journal journal;
    private final PayloadCodec codec;
    private final EngineRuntime runtime;
    private final String workerId;
    private final Duration leaseTtl;
    private final Duration pollInterval;
    private final Duration retention;
    private final EngineObserver observer;
    private volatile Instant lastLeaseRenewal;
    private final Semaphore slots;
    private final Map<String, WorkflowDefinition<?, ?>> definitions = new ConcurrentHashMap<>();
    // Ordered, so that a simulation visits sessions in the same order on every run.
    private final ConcurrentSkipListMap<Lease, Session> sessions = new ConcurrentSkipListMap<>(BY_WORKFLOW);
    private final SerialTask dispatcher;
    private volatile Set<String> workerTypes = Set.of();
    private volatile Runnable unsubscribe = () -> { };
    private volatile boolean stopped;
    private volatile boolean crashed;

    private TenazEngine(Builder builder) {
        this.journal = builder.journal;
        this.codec = builder.codec;
        this.runtime = builder.runtime != null ? builder.runtime : new ThreadedRuntime();
        this.workerId = builder.workerId;
        this.leaseTtl = builder.leaseTtl;
        this.pollInterval = builder.pollInterval;
        this.retention = builder.retention;
        this.observer = builder.observer;
        this.slots = new Semaphore(builder.maxConcurrentWorkflows);
        this.dispatcher = new SerialTask(runtime, guarded(this::claimAvailable));
    }

    public static Builder builder(Journal journal) {
        return new Builder(journal);
    }

    public <I, O> TenazEngine register(String type, Class<I> inputType, Class<O> outputType, Workflow<I, O> workflow) {
        definitions.put(type, new WorkflowDefinition<>(type, inputType, outputType, workflow));
        return this;
    }

    /** Starts claiming and running workflows of the registered types. */
    public TenazEngine startWorkers() {
        workerTypes = Set.copyOf(definitions.keySet());
        unsubscribe = journal.subscribe(this::onChange);
        every(pollInterval, dispatcher::request);
        every(pollInterval, this::fireTimers);
        // On its own schedule: nothing else the engine does may delay a heartbeat.
        every(leaseTtl.dividedBy(3), this::renewLeases);
        if (retention != null) {
            every(purgeInterval(), this::purgeEnded);
        }
        return this;
    }

    /**
     * Starts a workflow. Starting an id that already exists starts nothing and returns a handle
     * to the existing execution, so callers may retry this call freely.
     */
    public <O> WorkflowHandle<O> start(String type, String workflowId, Object input) {
        @SuppressWarnings("unchecked")
        WorkflowDefinition<Object, O> definition = (WorkflowDefinition<Object, O>) definitions.get(type);
        if (definition == null) {
            throw new IllegalArgumentException("unknown workflow type: " + type);
        }
        journal.create(workflowId, new Event.WorkflowStarted(type, codec.encode(input), runtime.clock().instant()));
        return new Handle<>(workflowId, definition.outputType());
    }

    public <O> WorkflowHandle<O> handle(String workflowId, Class<O> outputType) {
        return new Handle<>(workflowId, outputType);
    }

    /** Stops gracefully: workflows this engine was driving are handed back for others to claim. */
    @Override
    public void close() {
        stopped = true;
        unsubscribe.run();
        runtime.shutdown(true);
        for (Session session : sessions.values()) {
            session.dispose();
            try {
                journal.abandon(session.lease);
            } catch (RuntimeException e) {
                // The lease will expire on its own.
            }
        }
        sessions.clear();
    }

    /**
     * Simulates the death of the process: from this instant the engine writes nothing more to the
     * journal and releases nothing. Steps that are mid-flight may still finish their work in the
     * outside world, exactly as they could in a real crash.
     */
    public void crash() {
        crashed = true;
        stopped = true;
        unsubscribe.run();
        runtime.shutdown(false);
        sessions.values().forEach(Session::dispose);
    }

    private Journal live() {
        if (crashed) {
            throw new EngineDead();
        }
        return journal;
    }

    /** Wraps a task so that it does nothing once the engine stops and cannot fail the runtime. */
    private Runnable guarded(Runnable task) {
        return () -> {
            if (stopped) {
                return;
            }
            try {
                task.run();
            } catch (EngineDead e) {
                // crashed mid-task
            } catch (RuntimeException e) {
                // Shutdown interrupts tasks, and an interrupt inside a journal call looks like this.
                if (!stopped) {
                    LOG.log(System.Logger.Level.WARNING, "engine task failed; it will be retried", e);
                }
            }
        };
    }

    private void every(Duration interval, Runnable task) {
        Runnable guardedTask = guarded(task);
        runtime.execute(new Runnable() {
            @Override
            public void run() {
                guardedTask.run();
                if (!stopped) {
                    runtime.schedule(interval, this);
                }
            }
        });
    }

    /** Journal notifications are only a shortcut: polling finds everything they announce. */
    private void onChange(String workflowId, long version, boolean claimable) {
        if (stopped) {
            return;
        }
        Lease from = new Lease(workflowId, "", Long.MIN_VALUE);
        Lease to = new Lease(workflowId, "", Long.MAX_VALUE);
        sessions.subMap(from, true, to, true).values().forEach(session -> session.changed(version));
        if (claimable) {
            dispatcher.request();
        }
    }

    private void claimAvailable() {
        while (!stopped) {
            int wanted = Math.min(slots.drainPermits(), CLAIM_BATCH);
            if (wanted == 0) {
                return;
            }
            List<Lease> leases = List.of();
            try {
                leases = live().claim(workerId, workerTypes, leaseTtl, runtime.clock().instant(), wanted);
            } finally {
                slots.release(wanted - leases.size());
            }
            for (Lease lease : leases) {
                Session session = new Session(lease);
                sessions.put(lease, session);
                session.wake();
            }
            if (!leases.isEmpty()) {
                int claimed = leases.size();
                observe(o -> o.workflowsClaimed(claimed));
            }
            if (leases.size() < wanted) {
                return;
            }
        }
    }

    private void fireTimers() {
        if (live().fireDueTimers(runtime.clock().instant()) > 0) {
            dispatcher.request();
        }
    }

    /** A lease that cannot be renewed simply expires; the fencing epoch keeps that safe. */
    /** Often enough that nothing outlives its retention by more than a tenth of it. */
    private Duration purgeInterval() {
        Duration tenth = retention.dividedBy(10);
        if (tenth.compareTo(Duration.ofMinutes(1)) > 0) {
            return Duration.ofMinutes(1);
        }
        return tenth.compareTo(Duration.ofMillis(100)) < 0 ? Duration.ofMillis(100) : tenth;
    }

    private void purgeEnded() {
        Instant cutoff = runtime.clock().instant().minus(retention);
        while (!stopped && live().purge(cutoff, PURGE_BATCH) == PURGE_BATCH) {
            // more to delete: carry on in batches, so that no single statement holds many locks
        }
    }

    private void renewLeases() {
        List<Lease> held = List.copyOf(sessions.keySet());
        Instant before = runtime.clock().instant();
        Set<Lease> renewed = live().renew(held, leaseTtl, before);
        Instant after = runtime.clock().instant();
        lastLeaseRenewal = after;
        observe(o -> o.leasesRenewed(renewed.size(), Duration.between(before, after)));
        for (Lease lease : held) {
            Session session = sessions.get(lease);
            // A session that is handing its workflow back is not renewed either, and lost nothing.
            if (session != null && !renewed.contains(lease) && !session.lettingGo && !session.ended) {
                observe(o -> o.leaseLost(lease.workflowId()));
                session.fence();
            }
        }
    }

    private void observe(Consumer<EngineObserver> event) {
        try {
            event.accept(observer);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "observer failed", e);
        }
    }

    /** How many workflows this engine is driving right now. */
    public int activeWorkflows() {
        return sessions.size();
    }

    /** When this engine last renewed its leases, or empty if its workers have not done so yet. */
    public Optional<Instant> lastLeaseRenewal() {
        return Optional.ofNullable(lastLeaseRenewal);
    }

    public Duration leaseTtl() {
        return leaseTtl;
    }

    /** The journal this engine works on. */
    public Journal journal() {
        return journal;
    }

    /**
     * One workflow, for as long as this engine holds its lease. The session keeps the history it
     * has read, fetching only what is added to it, and keeps the workflow's code alive between
     * steps, so that only the first {@link #advance} replays from the top. Each advance feeds
     * the code the step outcomes that arrived since, journals those outcomes together with the
     * commands they led to, and starts the steps the code is now waiting on. The session ends
     * when the workflow finishes or is waiting only on timers and signals.
     */
    private final class Session {
        final Lease lease;
        final HistoryIndex history = new HistoryIndex();
        final Set<Integer> launched = new HashSet<>();
        final Set<Integer> startedChildren = new HashSet<>();
        final List<Runnable> cancellations = new ArrayList<>();
        final Queue<Event> arrived = new ConcurrentLinkedQueue<>();
        final List<Event> unjournaled = new ArrayList<>();
        final SerialTask advancing = new SerialTask(runtime, guarded(this::advance));
        // Held while the execution is in use, so that shutdown cannot discard it mid-advance.
        final ReentrantLock busy = new ReentrantLock();
        Replay execution;
        // Steps that were started and whose outcome is not in the journal yet.
        int outstanding;
        volatile long knownVersion;
        volatile boolean behind = true;
        volatile boolean fenced;
        volatile boolean ended;
        // Set while the session is giving the workflow up of its own accord.
        volatile boolean lettingGo;
        volatile String workflowType = "unknown";

        Session(Lease lease) {
            this.lease = lease;
        }

        void wake() {
            advancing.request();
        }

        void fence() {
            fenced = true;
            wake();
        }

        /** The journal announced a history of this length; ours may be missing events. */
        void changed(long version) {
            if (version > knownVersion) {
                behind = true;
                wake();
            }
        }

        private void advance() {
            busy.lock();
            try {
                advanceWhileBusy();
            } finally {
                busy.unlock();
            }
        }

        /** Called when the engine stops, for sessions that would otherwise never run again. */
        void dispose() {
            busy.lock();
            try {
                ended = true;
                discardExecution();
            } finally {
                busy.unlock();
            }
        }

        private void discardExecution() {
            if (execution != null) {
                execution.close();
                execution = null;
            }
        }

        private void advanceWhileBusy() {
            if (ended) {
                return;
            }
            String id = lease.workflowId();
            try {
                if (fenced) {
                    end(false);
                    return;
                }
                for (Event outcome = arrived.poll(); outcome != null; outcome = arrived.poll()) {
                    unjournaled.add(outcome);
                }
                if (behind) {
                    behind = false;
                    live().loadSince(id, history.version()).forEach(history::add);
                    knownVersion = history.version();
                    if (history.last() != null && history.last().endsWorkflow()) {
                        end(true);
                        return;
                    }
                }
                if (!(history.first() instanceof Event.WorkflowStarted started)) {
                    throw new IllegalStateException("workflow " + id + " has no history");
                }
                workflowType = started.workflowType();
                // Outcomes that are not durable yet are replayed as if they were, so that they
                // and the commands they lead to are journaled in a single append.
                HistoryIndex view = history;
                if (!unjournaled.isEmpty()) {
                    view = new HistoryIndex(history);
                    unjournaled.forEach(view::add);
                }
                if (execution == null) {
                    WorkflowDefinition<?, ?> definition = definitions.get(started.workflowType());
                    execution = new Replay(definition, id, codec, runtime.clock());
                }
                Replay.Result replay = execution.advance(view);
                List<Event> events = new ArrayList<>(unjournaled);
                events.addAll(replay.newEvents());
                List<Delivery> deliveries = new ArrayList<>();
                for (ChildCancellation child : replay.childrenToCancel()) {
                    deliveries.add(new Delivery(child.childId(), new Event.CancelRequested(child.reason())));
                }
                Event ending = switch (replay.outcome()) {
                    case Outcome.Completed completed -> new Event.WorkflowCompleted(completed.result());
                    case Outcome.Failed failed -> new Event.WorkflowFailed(failed.errorType(), failed.message());
                    case Outcome.Cancelled cancelled -> new Event.WorkflowCancelled(cancelled.reason());
                    case Outcome.Blocked blocked -> null;
                };
                if (ending != null) {
                    events.add(ending);
                    if (started.parentId() != null) {
                        // In the same atomic append, so a parent cannot miss its child's outcome.
                        deliveries.add(new Delivery(started.parentId(), outcomeForParent(started.parentSeq(), ending)));
                    }
                    lettingGo = true;
                    append(events, deliveries);
                    WorkflowStatus status = WorkflowStatus.after(ending);
                    observe(o -> o.workflowEnded(started.workflowType(), status));
                    end(true);
                    return;
                }
                // A child can only be cancelled if it exists. The ones being cancelled were
                // recorded before the cancellation, so creating them now cannot be premature.
                for (PendingChild child : replay.pendingChildren()) {
                    if (history.command(child.seq()) != null) {
                        startChild(child);
                    }
                }
                if (!events.isEmpty()) {
                    append(events, deliveries);
                    events.forEach(history::add);
                    for (Event outcome : unjournaled) {
                        if (outcome instanceof Event.StepCompleted || outcome instanceof Event.StepFailed) {
                            outstanding--;
                        }
                    }
                    unjournaled.clear();
                } else {
                    for (Delivery delivery : deliveries) {
                        live().appendExternal(delivery.workflowId(), delivery.event());
                    }
                }
                // Steps and children start only after the command that asks for them is durable.
                for (PendingStep step : replay.pendingSteps()) {
                    if (launched.add(step.seq())) {
                        outstanding++;
                        attempt(step, 1, step.retry().initialBackoff());
                    }
                }
                replay.pendingChildren().forEach(this::startChild);
                if (outstanding == 0 && arrived.isEmpty()) {
                    lettingGo = true;
                    if (live().park(lease, history.version())) {
                        end(true);
                    } else {
                        lettingGo = false;
                        behind = true;
                        wake();
                    }
                }
            } catch (VersionConflictException e) {
                lettingGo = false;
                // A signal or timer landed since we last read the history. The code has already
                // run ahead on outcomes that will now be journaled after that event, in an order
                // it did not see, so its execution is rebuilt from the journal.
                discardExecution();
                behind = true;
                wake();
            } catch (FencedException e) {
                observe(o -> o.leaseLost(id));
                end(true);
            } catch (EngineDead e) {
                ended = true;
                discardExecution();
            } catch (RuntimeException e) {
                if (!stopped) {
                    LOG.log(System.Logger.Level.WARNING, "session for workflow " + id + " aborted", e);
                }
                end(false);
            }
        }

        private void append(List<Event> events, List<Delivery> deliveries) {
            // Set first, so that the notification of our own append does not look like news.
            knownVersion = history.version() + events.size();
            Instant before = runtime.clock().instant();
            live().append(lease, history.version(), events, deliveries);
            Duration took = Duration.between(before, runtime.clock().instant());
            observe(o -> o.journalAppended(events.size(), took));
        }

        private Event outcomeForParent(int seq, Event ending) {
            return switch (ending) {
                case Event.WorkflowCompleted completed -> new Event.ChildCompleted(seq, completed.result());
                case Event.WorkflowFailed failed -> new Event.ChildFailed(seq, failed.errorType(), failed.message());
                case Event.WorkflowCancelled cancelled -> new Event.ChildFailed(
                        seq, WorkflowCancelledException.class.getName(), cancelled.reason());
                default -> throw new IllegalArgumentException("not an ending: " + ending);
            };
        }

        /**
         * Creates the child workflow. Creation is idempotent, so a session that takes the parent
         * over simply asks again; what it must check then is that the workflow under that id
         * really is this parent's child, or the parent would wait for a stranger.
         */
        private void startChild(PendingChild child) {
            if (!startedChildren.add(child.seq())) {
                return;
            }
            String parentId = lease.workflowId();
            Event.WorkflowStarted start = new Event.WorkflowStarted(child.workflowType(), child.input(),
                    runtime.clock().instant(), parentId, child.seq());
            if (live().createChild(lease, child.childId(), start)) {
                return;
            }
            boolean ours = live().started(child.childId())
                    .map(existing -> parentId.equals(existing.parentId())
                            && Integer.valueOf(child.seq()).equals(existing.parentSeq()))
                    .orElse(false);
            if (!ours) {
                arrived.add(new Event.ChildFailed(child.seq(), WORKFLOW_ID_IN_USE,
                        "a workflow with id '" + child.childId() + "' already exists"));
                wake();
            }
        }

        /** @param released whether the journal already knows this engine no longer owns the workflow */
        private void end(boolean released) {
            ended = true;
            discardExecution();
            synchronized (cancellations) {
                cancellations.forEach(Runnable::run);
            }
            sessions.remove(lease);
            try {
                if (!released && !crashed && !stopped) {
                    journal.abandon(lease);
                }
            } catch (RuntimeException e) {
                // The lease will expire on its own.
            } finally {
                slots.release();
                dispatcher.request();
            }
        }

        private void attempt(PendingStep step, int attempt, Duration backoff) {
            StepContext context = new StepContext(lease.workflowId() + "/" + step.seq(), attempt);
            // An attempt ends once: with its own outcome, or with the timeout, whichever is first.
            AtomicBoolean settled = new AtomicBoolean();
            Instant begun = runtime.clock().instant();
            Runnable cancel = runtime.call(() -> step.body().apply(context), (result, error) -> guarded(() -> {
                if (settled.compareAndSet(false, true)) {
                    settle(step, attempt, backoff, begun, result, error);
                }
            }).run());
            synchronized (cancellations) {
                cancellations.add(cancel);
            }
            Duration timeout = step.retry().attemptTimeout();
            if (timeout != null) {
                runtime.schedule(timeout, guarded(() -> {
                    if (settled.compareAndSet(false, true)) {
                        cancel.run();
                        settle(step, attempt, backoff, begun, null, new StepTimeoutException(step.name(), timeout));
                    }
                }));
            }
        }

        private void settle(PendingStep step, int attempt, Duration backoff, Instant begun, Object result,
                            Throwable error) {
            if (ended || error instanceof InterruptedException) {
                return;
            }
            boolean willRetry = error != null && !(error instanceof NonRetryableException)
                    && attempt < step.retry().maxAttempts();
            StepOutcome outcome = error == null ? StepOutcome.COMPLETED
                    : error instanceof StepTimeoutException ? StepOutcome.TIMED_OUT
                    : willRetry ? StepOutcome.RETRIED : StepOutcome.FAILED;
            Duration took = Duration.between(begun, runtime.clock().instant());
            observe(o -> o.stepAttempted(workflowType, step.name(), outcome, took));
            if (willRetry) {
                runtime.schedule(backoff, guarded(() -> {
                    if (!ended) {
                        attempt(step, attempt + 1, step.retry().next(backoff));
                    }
                }));
                return;
            }
            arrived.add(error == null ? encodeResult(step, result) : new Event.StepFailed(
                    step.seq(), error.getClass().getName(), String.valueOf(error.getMessage())));
            wake();
        }

        private Event encodeResult(PendingStep step, Object result) {
            try {
                return new Event.StepCompleted(step.seq(), codec.encode(result));
            } catch (RuntimeException e) {
                return new Event.StepFailed(step.seq(), e.getClass().getName(), String.valueOf(e.getMessage()));
            }
        }
    }

    private static final class EngineDead extends Error {
        EngineDead() {
            super(null, null, false, false);
        }
    }

    private final class Handle<O> implements WorkflowHandle<O> {
        private final String workflowId;
        private final Class<O> outputType;

        Handle(String workflowId, Class<O> outputType) {
            this.workflowId = workflowId;
            this.outputType = outputType;
        }

        @Override
        public String workflowId() {
            return workflowId;
        }

        @Override
        public O result(Duration timeout) throws TimeoutException, InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                History history = history();
                Event last = history.events().getLast();
                if (last instanceof Event.WorkflowCompleted completed) {
                    return codec.decode(completed.result(), outputType);
                }
                if (last instanceof Event.WorkflowFailed failed) {
                    throw new WorkflowFailedException(workflowId, failed.errorType(), failed.message());
                }
                if (last instanceof Event.WorkflowCancelled cancelled) {
                    throw new WorkflowCancelledException(cancelled.reason());
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("workflow " + workflowId + " still running after " + timeout);
                }
                journal.awaitChange(workflowId, history.version(), Duration.ofNanos(remaining));
            }
        }

        @Override
        public boolean isDone() {
            return history().status() != WorkflowStatus.RUNNING;
        }

        @Override
        public void signal(String name, Object payload) {
            journal.appendExternal(workflowId, new Event.SignalReceived(name, codec.encode(payload)));
        }

        @Override
        public void signal(String name, Object payload, String idempotencyKey) {
            journal.appendExternal(workflowId,
                    new Event.SignalReceived(name, codec.encode(payload), idempotencyKey));
        }

        @Override
        public void cancel(String reason) {
            journal.appendExternal(workflowId, new Event.CancelRequested(reason));
        }

        private History history() {
            return journal.load(workflowId)
                    .orElseThrow(() -> new IllegalArgumentException("unknown workflow: " + workflowId));
        }
    }

    public static final class Builder {
        private final Journal journal;
        private PayloadCodec codec = new JacksonCodec();
        private EngineRuntime runtime;
        private String workerId = "worker-" + UUID.randomUUID();
        private Duration leaseTtl = Duration.ofSeconds(10);
        private Duration pollInterval = Duration.ofMillis(50);
        private Duration retention;
        private EngineObserver observer = new EngineObserver() { };
        private int maxConcurrentWorkflows = 1000;

        private Builder(Journal journal) {
            this.journal = journal;
        }

        public Builder codec(PayloadCodec codec) {
            this.codec = codec;
            return this;
        }

        /** Replaces threads and wall-clock time, for running the engine under simulation. */
        public Builder runtime(EngineRuntime runtime) {
            this.runtime = runtime;
            return this;
        }

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        /** How long a dead engine's workflows stay stuck before another engine takes them over. */
        public Builder leaseTtl(Duration leaseTtl) {
            this.leaseTtl = leaseTtl;
            return this;
        }

        /** How often to look for work that no notification announced, such as expired leases. */
        /**
         * How long to keep a workflow after it ends. Once that time has passed the workflow and
         * its history are deleted, and its id can be started again. Without this, nothing is
         * ever deleted.
         */
        /** Receives the engine's events, for metrics. */
        public Builder observer(EngineObserver observer) {
            this.observer = observer;
            return this;
        }

        public Builder retention(Duration retention) {
            this.retention = retention;
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder maxConcurrentWorkflows(int maxConcurrentWorkflows) {
            this.maxConcurrentWorkflows = maxConcurrentWorkflows;
            return this;
        }

        public TenazEngine build() {
            return new TenazEngine(this);
        }
    }
}
