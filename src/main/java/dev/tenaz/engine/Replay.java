package dev.tenaz.engine;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.NonDeterminismError;
import dev.tenaz.api.PayloadCodec;
import dev.tenaz.api.RetryPolicy;
import dev.tenaz.api.StepAction;
import dev.tenaz.api.StepFailedException;
import dev.tenaz.api.StepFunction;
import dev.tenaz.api.WorkflowContext;
import dev.tenaz.engine.HistoryIndex.Resolution;
import dev.tenaz.journal.Event;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One execution of a workflow's code, kept alive for as long as someone drives it.
 *
 * <p>The code runs on its own virtual thread. Whatever the history already answers is answered
 * from it; when the code needs an answer that is not there yet, its thread parks, with the
 * workflow's stack intact. {@link #advance} hands it a longer history and lets it run until it
 * parks again or ends. The first advance is therefore a replay of everything recorded so far,
 * and each later one costs only the new work.
 *
 * <p>The two threads never run at the same time: the caller of {@code advance} waits while the
 * workflow thread runs, and the workflow thread waits otherwise. That keeps the execution as
 * deterministic as running the code inline would be.
 *
 * <p>Nothing is observable about an unresolved promise except by waiting on it, and resolved
 * promises are ordered by their position in the append-only history. So code that is fed a
 * history a piece at a time takes the same path as code replayed against the whole of it, which
 * is what lets another engine rebuild this execution from the journal alone.
 */
final class Replay implements AutoCloseable {

    sealed interface Outcome {
        record Completed(String result) implements Outcome {}

        record Failed(String errorType, String message) implements Outcome {}

        /** The code is waiting on steps, timers or signals. */
        record Blocked() implements Outcome {}
    }

    /**
     * @param newEvents    commands the code issued since the last advance that are not in the history
     * @param pendingSteps steps the code issued since the last advance that have no recorded outcome
     */
    record Result(Outcome outcome, List<Event> newEvents, List<PendingStep> pendingSteps) {}

    record PendingStep(int seq, String name, StepFunction<?> body, RetryPolicy retry) {}

    /** Unwinds the workflow code when its execution is discarded. */
    private static final class Abandoned extends Error {
        static final Abandoned INSTANCE = new Abandoned();

        private Abandoned() {
            super(null, null, false, false);
        }
    }

    private final String workflowId;
    private final PayloadCodec codec;
    private final Clock clock;
    private final Context ctx = new Context();
    private final Semaphore resume = new Semaphore(0);
    private final Semaphore yielded = new Semaphore(0);
    private final Thread thread;
    private boolean started;
    private volatile boolean closed;
    // Written by the workflow thread before it yields, read by the driver after.
    private Outcome outcome;
    private Throwable crash;
    private boolean nonDeterministic;
    private boolean parked;

    Replay(WorkflowDefinition<?, ?> definition, String workflowId, PayloadCodec codec, Clock clock) {
        this.workflowId = workflowId;
        this.codec = codec;
        this.clock = clock;
        this.thread = Thread.ofVirtual().name("tenaz-workflow-" + workflowId).unstarted(() -> run(definition));
    }

    /** Replays a whole history in one go. */
    static Result run(WorkflowDefinition<?, ?> definition, String workflowId, List<Event> history,
                      PayloadCodec codec, Clock clock) {
        try (Replay replay = new Replay(definition, workflowId, codec, clock)) {
            return replay.advance(HistoryIndex.of(history));
        }
    }

    /**
     * Runs the code against a history that extends the one it last saw, until it blocks or ends.
     * Once it has ended, or has been found non-deterministic, it cannot be advanced again.
     */
    Result advance(HistoryIndex history) {
        if (closed || outcome != null && !(outcome instanceof Outcome.Blocked)) {
            throw new IllegalStateException("workflow execution of " + workflowId + " is over");
        }
        if (!(history.first() instanceof Event.WorkflowStarted)) {
            throw new IllegalStateException("history of " + workflowId + " does not begin with WorkflowStarted");
        }
        ctx.history = history;
        ctx.newEvents = new ArrayList<>();
        ctx.pendingSteps = new ArrayList<>();
        if (!started) {
            started = true;
            thread.start();
        }
        resume.release();
        yielded.acquireUninterruptibly();
        if (crash != null) {
            throw new IllegalStateException("workflow code of " + workflowId + " died", crash);
        }
        if (!nonDeterministic && !(outcome instanceof Outcome.Failed)) {
            try {
                ctx.checkHistoryConsumed();
            } catch (NonDeterminismError e) {
                diverged(e);
            }
        }
        if (nonDeterministic) {
            // Whatever the code asked for after diverging from its history means nothing.
            return new Result(outcome, List.of(), List.of());
        }
        return new Result(outcome, ctx.newEvents, outcome instanceof Outcome.Failed ? List.of() : ctx.pendingSteps);
    }

    /** Discards the execution, unwinding the workflow code if it is parked. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parked) {
            resume.release();
            yielded.acquireUninterruptibly();
        }
    }

    private <I, O> void run(WorkflowDefinition<I, O> definition) {
        resume.acquireUninterruptibly();
        try {
            Event.WorkflowStarted started = (Event.WorkflowStarted) ctx.history.first();
            I input = codec.decode(started.input(), definition.inputType());
            O output = definition.workflow().run(ctx, input);
            outcome = new Outcome.Completed(codec.encode(output));
        } catch (Abandoned e) {
            // closed while parked; nobody wants an outcome
        } catch (NonDeterminismError e) {
            diverged(e);
        } catch (Exception e) {
            outcome = new Outcome.Failed(e.getClass().getName(), String.valueOf(e.getMessage()));
        } catch (Throwable e) {
            crash = e;
        } finally {
            yielded.release();
        }
    }

    private void diverged(NonDeterminismError e) {
        nonDeterministic = true;
        outcome = new Outcome.Failed(e.getClass().getName(), e.getMessage());
    }

    /** Called on the workflow thread: hands control back until the history has grown. */
    private void park() {
        outcome = new Outcome.Blocked();
        parked = true;
        yielded.release();
        resume.acquireUninterruptibly();
        parked = false;
        if (closed) {
            throw Abandoned.INSTANCE;
        }
    }

    private final class Promise<T> implements DurablePromise<T> {
        private final Supplier<Resolution> lookup;
        private final Function<Resolution, T> value;

        Promise(Supplier<Resolution> lookup, Function<Resolution, T> value) {
            this.lookup = lookup;
            this.value = value;
        }

        @Override
        public T get() {
            Resolution resolution = lookup.get();
            while (resolution == null) {
                park();
                resolution = lookup.get();
            }
            return value.apply(resolution);
        }
    }

    private final class Context implements WorkflowContext {
        private final Map<String, Integer> signalCursor = new HashMap<>();
        HistoryIndex history;
        List<Event> newEvents;
        List<PendingStep> pendingSteps;
        private int nextSeq;
        private int uuidCounter;

        /** Code that stops short of commands it issued in an earlier run has changed underneath us. */
        void checkHistoryConsumed() {
            int recorded = history.maxCommandSeq();
            if (recorded >= nextSeq) {
                throw new NonDeterminismError("history has command #" + recorded + " (" + history.command(recorded)
                        + ") but the workflow code only issued " + nextSeq + " commands");
            }
        }

        /** Code that swallowed {@link Abandoned} must not be able to carry on. */
        private void checkOpen() {
            if (closed) {
                throw Abandoned.INSTANCE;
            }
        }

        @Override
        public String workflowId() {
            return workflowId;
        }

        @Override
        public <T> T step(String name, Class<T> type, StepFunction<T> body) {
            return stepAsync(name, type, RetryPolicy.DEFAULT, body).get();
        }

        @Override
        public <T> T step(String name, Class<T> type, RetryPolicy retry, StepFunction<T> body) {
            return stepAsync(name, type, retry, body).get();
        }

        @Override
        public void run(String name, StepAction body) {
            step(name, Void.class, step -> {
                body.run(step);
                return null;
            });
        }

        @Override
        public <T> DurablePromise<T> stepAsync(String name, Class<T> type, StepFunction<T> body) {
            return stepAsync(name, type, RetryPolicy.DEFAULT, body);
        }

        @Override
        public <T> DurablePromise<T> stepAsync(String name, Class<T> type, RetryPolicy retry, StepFunction<T> body) {
            checkOpen();
            int seq = nextSeq++;
            Event recorded = history.command(seq);
            if (recorded == null) {
                newEvents.add(new Event.StepScheduled(seq, name));
            } else if (!(recorded instanceof Event.StepScheduled scheduled && scheduled.name().equals(name))) {
                throw mismatch(seq, "step '" + name + "'", recorded);
            }
            if (history.resolution(seq) == null) {
                pendingSteps.add(new PendingStep(seq, name, body, retry));
            }
            return new Promise<>(() -> history.resolution(seq), resolution -> switch (resolution.event()) {
                case Event.StepCompleted done -> codec.decode(done.result(), type);
                case Event.StepFailed failed ->
                        throw new StepFailedException(name, failed.errorType(), failed.message());
                default -> throw new IllegalStateException("unexpected " + resolution.event());
            });
        }

        @Override
        public void sleep(Duration duration) {
            timer(duration).get();
        }

        @Override
        public DurablePromise<Void> timer(Duration duration) {
            checkOpen();
            int seq = nextSeq++;
            Event recorded = history.command(seq);
            if (recorded == null) {
                newEvents.add(new Event.TimerStarted(seq, clock.instant().plus(duration)));
            } else if (!(recorded instanceof Event.TimerStarted)) {
                throw mismatch(seq, "timer", recorded);
            }
            return new Promise<>(() -> history.resolution(seq), resolution -> null);
        }

        @Override
        public <T> T awaitSignal(String name, Class<T> type) {
            return signal(name, type).get();
        }

        @Override
        public <T> DurablePromise<T> signal(String name, Class<T> type) {
            checkOpen();
            int position = signalCursor.merge(name, 1, Integer::sum) - 1;
            return new Promise<>(() -> history.signal(name, position),
                    resolution -> codec.decode(((Event.SignalReceived) resolution.event()).payload(), type));
        }

        @Override
        public DurablePromise<?> anyOf(DurablePromise<?>... promises) {
            while (true) {
                checkOpen();
                Promise<?> first = null;
                int firstAt = Integer.MAX_VALUE;
                for (DurablePromise<?> candidate : promises) {
                    Promise<?> promise = (Promise<?>) candidate;
                    Resolution resolution = promise.lookup.get();
                    if (resolution != null && resolution.position() < firstAt) {
                        first = promise;
                        firstAt = resolution.position();
                    }
                }
                if (first != null) {
                    return first;
                }
                park();
            }
        }

        @Override
        public <T> T sideEffect(Class<T> type, Supplier<T> supplier) {
            checkOpen();
            int seq = nextSeq++;
            Event recorded = history.command(seq);
            String value;
            if (recorded == null) {
                value = codec.encode(supplier.get());
                newEvents.add(new Event.SideEffectRecorded(seq, value));
            } else if (recorded instanceof Event.SideEffectRecorded sideEffect) {
                value = sideEffect.value();
            } else {
                throw mismatch(seq, "side effect", recorded);
            }
            // Always decoded from the journaled form, so the first run sees what replays will see.
            return codec.decode(value, type);
        }

        @Override
        public Instant now() {
            return sideEffect(Instant.class, clock::instant);
        }

        @Override
        public UUID randomUUID() {
            checkOpen();
            String name = workflowId + "/uuid/" + uuidCounter++;
            return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
        }

        private NonDeterminismError mismatch(int seq, String asked, Event recorded) {
            return new NonDeterminismError("command #" + seq + ": the workflow code asked for " + asked
                    + " but the history recorded " + recorded);
        }
    }
}
