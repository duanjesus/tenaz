package dev.tenaz.engine;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.NonDeterminismError;
import dev.tenaz.api.PayloadCodec;
import dev.tenaz.api.RetryPolicy;
import dev.tenaz.api.StepAction;
import dev.tenaz.api.StepFailedException;
import dev.tenaz.api.StepFunction;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowContext;
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
import java.util.function.Supplier;

/**
 * Runs workflow code against a history and reports how far it got.
 *
 * <p>The code runs from the top on the calling thread. Whatever the history already answers is
 * answered from it; the first time the code needs an answer that is not there yet, it is unwound
 * with {@link Suspended}. Nothing is observable about an unresolved promise except by waiting on
 * it, and resolved promises are ordered by their position in the append-only history, so a replay
 * against a longer history always retraces the path of a replay against a shorter one.
 */
final class Replay {

    sealed interface Outcome {
        record Completed(String result) implements Outcome {}

        record Failed(String errorType, String message) implements Outcome {}

        /** The code is waiting on steps, timers or signals. */
        record Blocked() implements Outcome {}
    }

    /**
     * @param newEvents    commands the code issued that the history does not contain yet
     * @param pendingSteps every step the code is counting on that has no recorded outcome
     */
    record Result(Outcome outcome, List<Event> newEvents, List<PendingStep> pendingSteps) {}

    record PendingStep(int seq, String name, StepFunction<?> body, RetryPolicy retry) {}

    private static final class Suspended extends Error {
        static final Suspended INSTANCE = new Suspended();

        private Suspended() {
            super(null, null, false, false);
        }
    }

    private Replay() {}

    static <I, O> Result run(WorkflowDefinition<I, O> definition, String workflowId, List<Event> history,
                             PayloadCodec codec, Clock clock) {
        if (history.isEmpty() || !(history.get(0) instanceof Event.WorkflowStarted started)) {
            throw new IllegalStateException("history of " + workflowId + " does not begin with WorkflowStarted");
        }
        Context ctx = new Context(workflowId, history, codec, clock);
        try {
            Outcome outcome;
            try {
                I input = codec.decode(started.input(), definition.inputType());
                O output = definition.workflow().run(ctx, input);
                outcome = new Outcome.Completed(codec.encode(output));
            } catch (Suspended e) {
                outcome = new Outcome.Blocked();
            } catch (Exception e) {
                return new Result(new Outcome.Failed(e.getClass().getName(), String.valueOf(e.getMessage())),
                        ctx.newEvents, List.of());
            }
            ctx.checkHistoryConsumed();
            return new Result(outcome, ctx.newEvents, ctx.pendingSteps);
        } catch (NonDeterminismError e) {
            return new Result(new Outcome.Failed(e.getClass().getName(), e.getMessage()), List.of(), List.of());
        }
    }

    private record Resolution(int index, Event event) {}

    private static final class Promise<T> implements DurablePromise<T> {
        final int resolvedAt;
        final Supplier<T> value;

        Promise(Resolution resolution, Supplier<T> value) {
            this.resolvedAt = resolution == null ? -1 : resolution.index();
            this.value = value;
        }

        @Override
        public T get() {
            if (resolvedAt < 0) {
                throw Suspended.INSTANCE;
            }
            return value.get();
        }
    }

    private static final class Context implements WorkflowContext {
        private final String workflowId;
        private final PayloadCodec codec;
        private final Clock clock;
        private final Map<Integer, Event> commands = new HashMap<>();
        private final Map<Integer, Resolution> resolutions = new HashMap<>();
        private final Map<String, List<Resolution>> signals = new HashMap<>();
        private final Map<String, Integer> signalCursor = new HashMap<>();
        final List<Event> newEvents = new ArrayList<>();
        final List<PendingStep> pendingSteps = new ArrayList<>();
        private int nextSeq;
        private int uuidCounter;

        Context(String workflowId, List<Event> history, PayloadCodec codec, Clock clock) {
            this.workflowId = workflowId;
            this.codec = codec;
            this.clock = clock;
            for (int i = 0; i < history.size(); i++) {
                Event event = history.get(i);
                switch (event) {
                    case Event.StepScheduled e -> commands.put(e.seq(), e);
                    case Event.TimerStarted e -> commands.put(e.seq(), e);
                    case Event.SideEffectRecorded e -> commands.put(e.seq(), e);
                    case Event.StepCompleted e -> resolutions.put(e.seq(), new Resolution(i, e));
                    case Event.StepFailed e -> resolutions.put(e.seq(), new Resolution(i, e));
                    case Event.TimerFired e -> resolutions.put(e.seq(), new Resolution(i, e));
                    case Event.SignalReceived e ->
                            signals.computeIfAbsent(e.name(), k -> new ArrayList<>()).add(new Resolution(i, e));
                    default -> { }
                }
            }
        }

        /** Code that stops short of commands it issued in an earlier run has changed underneath us. */
        void checkHistoryConsumed() {
            for (int seq : commands.keySet()) {
                if (seq >= nextSeq) {
                    throw new NonDeterminismError("history has command #" + seq + " (" + commands.get(seq)
                            + ") but the workflow code only issued " + nextSeq + " commands");
                }
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
            int seq = nextSeq++;
            Event recorded = commands.get(seq);
            if (recorded == null) {
                newEvents.add(new Event.StepScheduled(seq, name));
            } else if (!(recorded instanceof Event.StepScheduled scheduled && scheduled.name().equals(name))) {
                throw mismatch(seq, "step '" + name + "'", recorded);
            }
            Resolution resolution = resolutions.get(seq);
            if (resolution == null) {
                pendingSteps.add(new PendingStep(seq, name, body, retry));
            }
            return new Promise<>(resolution, () -> switch (resolution.event()) {
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
            int seq = nextSeq++;
            Event recorded = commands.get(seq);
            if (recorded == null) {
                newEvents.add(new Event.TimerStarted(seq, clock.instant().plus(duration)));
            } else if (!(recorded instanceof Event.TimerStarted)) {
                throw mismatch(seq, "timer", recorded);
            }
            return new Promise<>(resolutions.get(seq), () -> null);
        }

        @Override
        public <T> T awaitSignal(String name, Class<T> type) {
            return signal(name, type).get();
        }

        @Override
        public <T> DurablePromise<T> signal(String name, Class<T> type) {
            int position = signalCursor.merge(name, 1, Integer::sum) - 1;
            List<Resolution> received = signals.getOrDefault(name, List.of());
            Resolution resolution = position < received.size() ? received.get(position) : null;
            return new Promise<>(resolution,
                    () -> codec.decode(((Event.SignalReceived) resolution.event()).payload(), type));
        }

        @Override
        public DurablePromise<?> anyOf(DurablePromise<?>... promises) {
            Promise<?> first = null;
            for (DurablePromise<?> candidate : promises) {
                Promise<?> promise = (Promise<?>) candidate;
                if (promise.resolvedAt >= 0 && (first == null || promise.resolvedAt < first.resolvedAt)) {
                    first = promise;
                }
            }
            if (first == null) {
                throw Suspended.INSTANCE;
            }
            return first;
        }

        @Override
        public <T> T sideEffect(Class<T> type, Supplier<T> supplier) {
            int seq = nextSeq++;
            Event recorded = commands.get(seq);
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
            String name = workflowId + "/uuid/" + uuidCounter++;
            return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
        }

        private NonDeterminismError mismatch(int seq, String asked, Event recorded) {
            return new NonDeterminismError("command #" + seq + ": the workflow code asked for " + asked
                    + " but the history recorded " + recorded);
        }
    }
}
