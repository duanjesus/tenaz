package dev.tenaz.engine;

import dev.tenaz.api.JacksonCodec;
import dev.tenaz.api.NonRetryableException;
import dev.tenaz.api.PayloadCodec;
import dev.tenaz.api.StepContext;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowFailedException;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.Replay.Outcome;
import dev.tenaz.engine.Replay.PendingStep;
import dev.tenaz.journal.Event;
import dev.tenaz.journal.Journal;
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
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

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
    private static final Comparator<Lease> BY_WORKFLOW =
            Comparator.comparing(Lease::workflowId).thenComparingLong(Lease::epoch);

    private final Journal journal;
    private final PayloadCodec codec;
    private final EngineRuntime runtime;
    private final String workerId;
    private final Duration leaseTtl;
    private final Duration pollInterval;
    private final Semaphore slots;
    private final Map<String, WorkflowDefinition<?, ?>> definitions = new ConcurrentHashMap<>();
    // Ordered, so that a simulation visits sessions in the same order on every run.
    private final ConcurrentSkipListMap<Lease, Session> sessions = new ConcurrentSkipListMap<>(BY_WORKFLOW);
    private final SerialTask dispatcher;
    private volatile Set<String> workerTypes = Set.of();
    private volatile Runnable unsubscribe = () -> { };
    private volatile Instant nextRenewal = Instant.MIN;
    private volatile boolean stopped;
    private volatile boolean crashed;

    private TenazEngine(Builder builder) {
        this.journal = builder.journal;
        this.codec = builder.codec;
        this.runtime = builder.runtime != null ? builder.runtime : new ThreadedRuntime();
        this.workerId = builder.workerId;
        this.leaseTtl = builder.leaseTtl;
        this.pollInterval = builder.pollInterval;
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
        Duration renewEvery = leaseTtl.dividedBy(3);
        every(pollInterval, dispatcher::request);
        every(pollInterval.compareTo(renewEvery) < 0 ? pollInterval : renewEvery, this::housekeep);
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
            if (leases.size() < wanted) {
                return;
            }
        }
    }

    /** Fires due timers and keeps the leases of live sessions from expiring. */
    private void housekeep() {
        Instant now = runtime.clock().instant();
        if (live().fireDueTimers(now) > 0) {
            dispatcher.request();
        }
        if (!now.isBefore(nextRenewal)) {
            nextRenewal = now.plus(leaseTtl.dividedBy(3));
            // A lease that cannot be renewed simply expires; the fencing epoch keeps that safe.
            List<Lease> held = List.copyOf(sessions.keySet());
            Set<Lease> renewed = live().renew(held, leaseTtl, runtime.clock().instant());
            for (Lease lease : held) {
                Session session = sessions.get(lease);
                if (session != null && !renewed.contains(lease)) {
                    session.fence();
                }
            }
        }
    }

    /**
     * One workflow, for as long as this engine holds its lease. The session keeps the history it
     * has read and only ever fetches what was added to it. Each {@link #advance} replays the code
     * against that history plus the step outcomes that arrived since, journals those outcomes
     * together with the commands they led to, and starts the steps the code is now waiting on.
     * The session ends when the workflow finishes or is waiting only on timers and signals.
     */
    private final class Session {
        final Lease lease;
        final HistoryIndex history = new HistoryIndex();
        final Set<Integer> launched = new HashSet<>();
        final List<Runnable> cancellations = new ArrayList<>();
        final Queue<Event> arrived = new ConcurrentLinkedQueue<>();
        final List<Event> unjournaled = new ArrayList<>();
        final SerialTask advancing = new SerialTask(runtime, guarded(this::advance));
        // Steps that were started and whose outcome is not in the journal yet.
        int outstanding;
        volatile long knownVersion;
        volatile boolean behind = true;
        volatile boolean fenced;
        volatile boolean ended;

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
                    if (history.last() instanceof Event.WorkflowCompleted
                            || history.last() instanceof Event.WorkflowFailed) {
                        end(true);
                        return;
                    }
                }
                if (!(history.first() instanceof Event.WorkflowStarted started)) {
                    throw new IllegalStateException("workflow " + id + " has no history");
                }
                // Outcomes that are not durable yet are replayed as if they were, so that they
                // and the commands they lead to are journaled in a single append.
                HistoryIndex view = history;
                if (!unjournaled.isEmpty()) {
                    view = new HistoryIndex(history);
                    unjournaled.forEach(view::add);
                }
                WorkflowDefinition<?, ?> definition = definitions.get(started.workflowType());
                Replay.Result replay = Replay.run(definition, id, view, codec, runtime.clock());
                List<Event> events = new ArrayList<>(unjournaled);
                events.addAll(replay.newEvents());
                switch (replay.outcome()) {
                    case Outcome.Completed completed -> {
                        events.add(new Event.WorkflowCompleted(completed.result()));
                        append(events);
                        end(true);
                    }
                    case Outcome.Failed failed -> {
                        events.add(new Event.WorkflowFailed(failed.errorType(), failed.message()));
                        append(events);
                        end(true);
                    }
                    case Outcome.Blocked blocked -> {
                        if (!events.isEmpty()) {
                            append(events);
                            events.forEach(history::add);
                            outstanding -= unjournaled.size();
                            unjournaled.clear();
                        }
                        // Steps start only after their StepScheduled is durable.
                        for (PendingStep step : replay.pendingSteps()) {
                            if (launched.add(step.seq())) {
                                outstanding++;
                                attempt(step, 1, step.retry().initialBackoff());
                            }
                        }
                        if (outstanding == 0) {
                            if (live().park(lease, history.version())) {
                                end(true);
                            } else {
                                behind = true;
                                wake();
                            }
                        }
                    }
                }
            } catch (VersionConflictException e) {
                // A signal or timer landed since we last read the history: read it and replay.
                behind = true;
                wake();
            } catch (FencedException e) {
                end(true);
            } catch (EngineDead e) {
                ended = true;
            } catch (RuntimeException e) {
                if (!stopped) {
                    LOG.log(System.Logger.Level.WARNING, "session for workflow " + id + " aborted", e);
                }
                end(false);
            }
        }

        private void append(List<Event> events) {
            // Set first, so that the notification of our own append does not look like news.
            knownVersion = history.version() + events.size();
            live().append(lease, history.version(), events);
        }

        /** @param released whether the journal already knows this engine no longer owns the workflow */
        private void end(boolean released) {
            ended = true;
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
            Runnable cancel = runtime.call(() -> step.body().apply(context), (result, error) -> guarded(() -> {
                if (ended) {
                    return;
                }
                if (error == null) {
                    arrived.add(encodeResult(step, result));
                } else if (error instanceof InterruptedException) {
                    return;
                } else if (error instanceof NonRetryableException || attempt >= step.retry().maxAttempts()) {
                    arrived.add(new Event.StepFailed(
                            step.seq(), error.getClass().getName(), String.valueOf(error.getMessage())));
                } else {
                    runtime.schedule(backoff, guarded(() -> {
                        if (!ended) {
                            attempt(step, attempt + 1, step.retry().next(backoff));
                        }
                    }));
                    return;
                }
                wake();
            }).run());
            synchronized (cancellations) {
                cancellations.add(cancel);
            }
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
