package dev.tenaz.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.JacksonCodec;
import dev.tenaz.api.NonDeterminismError;
import dev.tenaz.api.Workflow;
import dev.tenaz.engine.Replay.Outcome;
import dev.tenaz.journal.Event;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReplayTest {

    private static final Event STARTED = new Event.WorkflowStarted("test", "\"in\"", Instant.EPOCH);

    private static Replay.Result replay(Workflow<String, String> workflow, Event... history) {
        var definition = new WorkflowDefinition<>("test", String.class, String.class, workflow);
        return Replay.run(definition, "wf-1", List.of(history), new JacksonCodec(), Clock.systemUTC());
    }

    @Test
    void firstRunEmitsTheCommandAndBlocks() {
        Replay.Result result = replay((ctx, in) -> ctx.step("a", String.class, step -> "x"), STARTED);

        assertInstanceOf(Outcome.Blocked.class, result.outcome());
        assertEquals(List.of(new Event.StepScheduled(0, "a")), result.newEvents());
        assertEquals(1, result.pendingSteps().size());
    }

    @Test
    void recordedResultsAreReturnedWithoutRunningTheStep() {
        Replay.Result result = replay(
                (ctx, in) -> ctx.step("a", String.class, step -> {
                    throw new AssertionError("must not run");
                }),
                STARTED, new Event.StepScheduled(0, "a"), new Event.StepCompleted(0, "\"recorded\""));

        assertEquals(new Outcome.Completed("\"recorded\""), result.outcome());
        assertTrue(result.newEvents().isEmpty());
    }

    @Test
    void codeThatAsksForADifferentStepIsRejected() {
        Replay.Result result = replay(
                (ctx, in) -> ctx.step("renamed", String.class, step -> "x"),
                STARTED, new Event.StepScheduled(0, "a"), new Event.StepCompleted(0, "\"recorded\""));

        Outcome.Failed failed = assertInstanceOf(Outcome.Failed.class, result.outcome());
        assertEquals(NonDeterminismError.class.getName(), failed.errorType());
    }

    @Test
    void codeThatDroppedACommandIsRejected() {
        Replay.Result result = replay(
                (ctx, in) -> "done",
                STARTED, new Event.StepScheduled(0, "a"));

        Outcome.Failed failed = assertInstanceOf(Outcome.Failed.class, result.outcome());
        assertEquals(NonDeterminismError.class.getName(), failed.errorType());
    }

    @Test
    void aLiveExecutionContinuesFromWhereItStoppedInsteadOfStartingOver() {
        AtomicInteger entries = new AtomicInteger();
        var definition = new WorkflowDefinition<>("test", String.class, String.class, (ctx, in) -> {
            entries.incrementAndGet();
            String a = ctx.step("a", String.class, step -> "unused");
            return a + ctx.step("b", String.class, step -> "unused");
        });
        HistoryIndex history = HistoryIndex.of(List.of(STARTED));

        try (Replay replay = new Replay(definition, "wf-1", new JacksonCodec(), Clock.systemUTC())) {
            Replay.Result first = replay.advance(history);
            assertEquals(List.of(new Event.StepScheduled(0, "a")), first.newEvents());

            first.newEvents().forEach(history::add);
            history.add(new Event.StepCompleted(0, "\"1\""));
            Replay.Result second = replay.advance(history);
            assertEquals(List.of(new Event.StepScheduled(1, "b")), second.newEvents());

            second.newEvents().forEach(history::add);
            history.add(new Event.StepCompleted(1, "\"2\""));
            assertEquals(new Outcome.Completed("\"12\""), replay.advance(history).outcome());
        }
        assertEquals(1, entries.get());
    }

    @Test
    void closingAParkedExecutionUnwindsTheWorkflowCode() {
        AtomicBoolean unwound = new AtomicBoolean();
        var definition = new WorkflowDefinition<>("test", String.class, String.class, (ctx, in) -> {
            try {
                return ctx.step("a", String.class, step -> "unused");
            } finally {
                unwound.set(true);
            }
        });

        Replay replay = new Replay(definition, "wf-1", new JacksonCodec(), Clock.systemUTC());
        assertInstanceOf(Outcome.Blocked.class, replay.advance(HistoryIndex.of(List.of(STARTED))).outcome());
        assertFalse(unwound.get());

        replay.close();
        assertTrue(unwound.get());
    }

    @Test
    void anyOfWinnerIsDecidedByHistoryOrderNotByArgumentOrder() {
        Workflow<String, String> race = (ctx, in) -> {
            DurablePromise<String> step = ctx.stepAsync("slow", String.class, s -> "x");
            DurablePromise<Void> timer = ctx.timer(Duration.ofSeconds(1));
            return ctx.anyOf(step, timer) == timer ? "timer" : "step";
        };
        Event scheduled = new Event.StepScheduled(0, "slow");
        Event timerStarted = new Event.TimerStarted(1, Instant.EPOCH);

        Replay.Result timerFirst = replay(race, STARTED, scheduled, timerStarted,
                new Event.TimerFired(1), new Event.StepCompleted(0, "\"x\""));
        Replay.Result stepFirst = replay(race, STARTED, scheduled, timerStarted,
                new Event.StepCompleted(0, "\"x\""), new Event.TimerFired(1));

        assertEquals(new Outcome.Completed("\"timer\""), timerFirst.outcome());
        assertEquals(new Outcome.Completed("\"step\""), stepFirst.outcome());
    }
}
