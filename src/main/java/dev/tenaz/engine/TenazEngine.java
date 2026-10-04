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
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A node of the durable execution engine. Any number of engines may share one {@link Journal};
 * each workflow is driven by whichever engine holds its lease, and is picked up by another one
 * when that engine dies.
 *
 * <p>An engine whose workers were never started is a client: it can start, signal and await
 * workflows that other engines run.
 */
public final class TenazEngine implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(TenazEngine.class.getName());

    private final Journal journal;
    private final PayloadCodec codec;
    private final Clock clock;
    private final String workerId;
    private final Duration leaseTtl;
    private final Duration pollInterval;
    private final Semaphore slots;
    private final Semaphore wakeup = new Semaphore(0);
    private final Map<String, WorkflowDefinition<?, ?>> definitions = new ConcurrentHashMap<>();
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean stopped;
    private volatile boolean crashed;

    private TenazEngine(Builder builder) {
        this.journal = builder.journal;
        this.codec = builder.codec;
        this.clock = builder.clock;
        this.workerId = builder.workerId;
        this.leaseTtl = builder.leaseTtl;
        this.pollInterval = builder.pollInterval;
        this.slots = new Semaphore(builder.maxConcurrentWorkflows);
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
        Set<String> types = Set.copyOf(definitions.keySet());
        executor.submit(() -> dispatch(types));
        executor.submit(this::housekeep);
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
        journal.create(workflowId, new Event.WorkflowStarted(type, codec.encode(input), clock.instant()));
        wakeup.release();
        return new Handle<>(workflowId, definition.outputType());
    }

    public <O> WorkflowHandle<O> handle(String workflowId, Class<O> outputType) {
        return new Handle<>(workflowId, outputType);
    }

    /** Stops gracefully: workflows this engine was driving are handed back for others to claim. */
    @Override
    public void close() {
        stopped = true;
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Simulates the death of the process: from this instant the engine writes nothing more to the
     * journal and releases nothing. Steps that are mid-flight may still finish their work in the
     * outside world, exactly as they could in a real crash.
     */
    public void crash() {
        crashed = true;
        stopped = true;
        executor.shutdownNow();
    }

    private Journal live() {
        if (crashed) {
            throw new EngineDead();
        }
        return journal;
    }

    private void dispatch(Set<String> types) {
        try {
            while (!stopped) {
                slots.acquire();
                Optional<Lease> lease = Optional.empty();
                try {
                    lease = live().claim(workerId, types, leaseTtl, clock.instant());
                } finally {
                    if (lease.isEmpty()) {
                        slots.release();
                    }
                }
                if (lease.isEmpty()) {
                    wakeup.tryAcquire(pollInterval.toNanos(), TimeUnit.NANOSECONDS);
                    wakeup.drainPermits();
                    continue;
                }
                Session session = new Session(lease.get());
                sessions.add(session);
                try {
                    executor.submit(() -> runSession(session));
                } catch (RejectedExecutionException e) {
                    sessions.remove(session);
                    slots.release();
                    return;
                }
            }
        } catch (InterruptedException | EngineDead e) {
            // stopping
        }
    }

    /** Fires due timers and keeps the leases of live sessions from expiring. */
    private void housekeep() {
        Duration renewEvery = leaseTtl.dividedBy(3);
        Duration tick = pollInterval.compareTo(renewEvery) < 0 ? pollInterval : renewEvery;
        long nextRenew = System.nanoTime();
        try {
            while (!stopped) {
                if (live().fireDueTimers(clock.instant()) > 0) {
                    wakeup.release();
                }
                if (System.nanoTime() - nextRenew >= 0) {
                    for (Session session : sessions) {
                        if (!live().renew(session.lease, leaseTtl, clock.instant())) {
                            session.fence();
                        }
                    }
                    nextRenew = System.nanoTime() + renewEvery.toNanos();
                }
                Thread.sleep(tick);
            }
        } catch (InterruptedException | EngineDead e) {
            // stopping
        }
    }

    /**
     * Drives one workflow for as long as this engine holds its lease: replay, journal the new
     * commands, run the steps the code is waiting on, and replay again when anything changes.
     * The session ends when the workflow finishes or is waiting only on timers and signals.
     */
    private void runSession(Session session) {
        Lease lease = session.lease;
        String id = lease.workflowId();
        session.thread = Thread.currentThread();
        boolean released = false;
        try {
            while (!stopped && !session.fenced) {
                History history = live().load(id).orElseThrow();
                if (history.status() != WorkflowStatus.RUNNING) {
                    released = true;
                    return;
                }
                WorkflowDefinition<?, ?> definition = definitions.get(history.workflowType());
                Replay.Result replay = Replay.run(definition, id, history.events(), codec, clock);
                if (stopped || session.fenced) {
                    // An interrupt may have reached the workflow code; its outcome means nothing.
                    return;
                }
                long version = history.version();
                try {
                    switch (replay.outcome()) {
                        case Outcome.Completed completed -> {
                            finish(lease, version, replay.newEvents(), new Event.WorkflowCompleted(completed.result()));
                            released = true;
                            return;
                        }
                        case Outcome.Failed failed -> {
                            finish(lease, version, replay.newEvents(),
                                    new Event.WorkflowFailed(failed.errorType(), failed.message()));
                            released = true;
                            return;
                        }
                        case Outcome.Blocked blocked -> {
                            if (!replay.newEvents().isEmpty()) {
                                live().appendDecisions(lease, version, replay.newEvents());
                                version += replay.newEvents().size();
                            }
                            // Steps start only after their StepScheduled is durable.
                            for (PendingStep step : replay.pendingSteps()) {
                                if (session.launched.add(step.seq())) {
                                    session.inFlight.incrementAndGet();
                                    session.steps.add(executor.submit(() -> runStep(session, step)));
                                }
                            }
                            if (session.inFlight.get() == 0) {
                                // A step that finished after we loaded the history has moved the
                                // version, which makes park refuse and sends us round again.
                                if (live().park(lease, version)) {
                                    released = true;
                                    return;
                                }
                                continue;
                            }
                            while (!live().awaitChange(id, version, pollInterval)) {
                                if (stopped || session.fenced) {
                                    return;
                                }
                            }
                        }
                    }
                } catch (VersionConflictException e) {
                    // A signal, timer or step result landed mid-replay: replay against the new history.
                }
            }
        } catch (InterruptedException | FencedException | EngineDead | RejectedExecutionException e) {
            // lease lost or engine stopping
        } catch (RuntimeException | Error e) {
            LOG.log(System.Logger.Level.ERROR, "session for workflow " + id + " aborted", e);
        } finally {
            session.steps.forEach(step -> step.cancel(true));
            sessions.remove(session);
            if (!released && !crashed) {
                journal.abandon(lease);
            }
            slots.release();
        }
    }

    private void finish(Lease lease, long version, List<Event> newEvents, Event terminal) {
        List<Event> events = new ArrayList<>(newEvents);
        events.add(terminal);
        live().appendDecisions(lease, version, events);
    }

    private void runStep(Session session, PendingStep step) {
        try {
            Event outcome = execute(session.lease.workflowId(), step);
            live().appendStepResult(session.lease, outcome);
        } catch (InterruptedException | FencedException | EngineDead e) {
            // The session is gone. Whoever owns the workflow now will run the step again.
        } catch (RuntimeException | Error e) {
            // The outcome was not journaled, so the session must not park as if it had been.
            LOG.log(System.Logger.Level.ERROR, "could not journal step '" + step.name() + "'", e);
            session.fence();
        } finally {
            session.inFlight.decrementAndGet();
        }
    }

    private Event execute(String workflowId, PendingStep step) throws InterruptedException {
        String idempotencyKey = workflowId + "/" + step.seq();
        Duration backoff = step.retry().initialBackoff();
        for (int attempt = 1; ; attempt++) {
            try {
                Object result = step.body().apply(new StepContext(idempotencyKey, attempt));
                return new Event.StepCompleted(step.seq(), codec.encode(result));
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                if (e instanceof NonRetryableException || attempt >= step.retry().maxAttempts()) {
                    return new Event.StepFailed(step.seq(), e.getClass().getName(), String.valueOf(e.getMessage()));
                }
                Thread.sleep(backoff);
                backoff = step.retry().next(backoff);
            }
        }
    }

    private static final class Session {
        final Lease lease;
        final Set<Integer> launched = new HashSet<>();
        final List<Future<?>> steps = new ArrayList<>();
        final AtomicInteger inFlight = new AtomicInteger();
        volatile Thread thread;
        volatile boolean fenced;

        Session(Lease lease) {
            this.lease = lease;
        }

        void fence() {
            fenced = true;
            Thread owner = thread;
            if (owner != null) {
                owner.interrupt();
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
            wakeup.release();
        }

        private History history() {
            return journal.load(workflowId)
                    .orElseThrow(() -> new IllegalArgumentException("unknown workflow: " + workflowId));
        }
    }

    public static final class Builder {
        private final Journal journal;
        private PayloadCodec codec = new JacksonCodec();
        private Clock clock = Clock.systemUTC();
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

        public Builder clock(Clock clock) {
            this.clock = clock;
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
