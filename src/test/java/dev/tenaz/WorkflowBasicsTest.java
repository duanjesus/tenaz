package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.RetryPolicy;
import dev.tenaz.api.StepFailedException;
import dev.tenaz.api.WorkflowFailedException;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WorkflowBasicsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    protected final Journal journal = createJournal();

    protected Journal createJournal() {
        return new InMemoryJournal();
    }

    private final TenazEngine engine = TenazEngine.builder(journal)
            .pollInterval(Duration.ofMillis(5))
            .build();

    @AfterEach
    void stop() {
        engine.close();
    }

    @Test
    void stepsRunOnceHoweverOftenTheWorkflowIsReplayed() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        engine.register("sum", Integer.class, Integer.class, (ctx, input) -> {
            runs.incrementAndGet();
            int a = ctx.step("first", Integer.class, step -> input + first.incrementAndGet());
            int b = ctx.step("second", Integer.class, step -> a + second.incrementAndGet());
            return a + b;
        }).startWorkers();

        assertEquals(23, engine.<Integer>start("sum", "sum-1", 10).result(TIMEOUT));
        assertEquals(1, first.get());
        assertEquals(1, second.get());
        assertEquals(1, runs.get(), "the code keeps its place between steps instead of being replayed");
    }

    @Test
    void startingTheSameIdTwiceRunsItOnce() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        engine.register("once", String.class, Integer.class,
                (ctx, input) -> ctx.step("count", Integer.class, step -> executions.incrementAndGet()))
                .startWorkers();

        WorkflowHandle<Integer> a = engine.start("once", "same-id", "x");
        WorkflowHandle<Integer> b = engine.start("once", "same-id", "x");

        assertEquals(1, a.result(TIMEOUT));
        assertEquals(1, b.result(TIMEOUT));
        assertEquals(1, executions.get());
    }

    @Test
    void asyncStepsRunInParallel() throws Exception {
        CyclicBarrier allThreeRunning = new CyclicBarrier(3);
        engine.register("fanout", Integer.class, Integer.class, (ctx, input) -> {
            DurablePromise<Integer> a = ctx.stepAsync("a", Integer.class, step -> meet(allThreeRunning, 1));
            DurablePromise<Integer> b = ctx.stepAsync("b", Integer.class, step -> meet(allThreeRunning, 2));
            DurablePromise<Integer> c = ctx.stepAsync("c", Integer.class, step -> meet(allThreeRunning, 3));
            return a.get() + b.get() + c.get();
        }).startWorkers();

        assertEquals(6, engine.<Integer>start("fanout", "fanout-1", 0).result(TIMEOUT));
    }

    private static int meet(CyclicBarrier barrier, int value) throws Exception {
        barrier.await(5, TimeUnit.SECONDS);
        return value;
    }

    @Test
    void sleepIsDurableAndTimeIsRecorded() throws Exception {
        engine.register("nap", String.class, Long.class, (ctx, input) -> {
            Instant before = ctx.now();
            ctx.sleep(Duration.ofMillis(80));
            Instant after = ctx.now();
            return Duration.between(before, after).toMillis();
        }).startWorkers();

        long slept = engine.<Long>start("nap", "nap-1", "x").result(TIMEOUT);
        assertTrue(slept >= 80, "slept " + slept + "ms");
    }

    @Test
    void sideEffectsAndUuidsAreStableAcrossReplays() throws Exception {
        AtomicInteger draws = new AtomicInteger();
        engine.register("stable", String.class, String.class, (ctx, input) -> {
            int drawn = ctx.sideEffect(Integer.class, draws::incrementAndGet);
            UUID id = ctx.randomUUID();
            UUID seen = ctx.step("echo", UUID.class, step -> id);
            ctx.sleep(Duration.ofMillis(10));
            return drawn + ":" + id.equals(seen) + ":" + id.equals(ctx.randomUUID());
        }).startWorkers();

        assertEquals("1:true:false", engine.<String>start("stable", "stable-1", "x").result(TIMEOUT));
        assertEquals(1, draws.get());
    }

    @Test
    void workflowWaitsForSignal() throws Exception {
        engine.register("approval", String.class, String.class,
                (ctx, input) -> input + " approved by " + ctx.awaitSignal("approve", String.class))
                .startWorkers();

        WorkflowHandle<String> handle = engine.start("approval", "approval-1", "order 7");
        Thread.sleep(50);
        assertTrue(!handle.isDone());
        handle.signal("approve", "ana");

        assertEquals("order 7 approved by ana", handle.result(TIMEOUT));
    }

    @Test
    void anyOfPicksWhicheverHappensFirst() throws Exception {
        engine.register("deadline", Integer.class, String.class, (ctx, timeoutMillis) -> {
            DurablePromise<String> approval = ctx.signal("approve", String.class);
            DurablePromise<Void> deadline = ctx.timer(Duration.ofMillis(timeoutMillis));
            return ctx.anyOf(approval, deadline) == approval ? "approved by " + approval.get() : "timed out";
        }).startWorkers();

        WorkflowHandle<String> expired = engine.start("deadline", "deadline-expired", 50);
        WorkflowHandle<String> approved = engine.start("deadline", "deadline-approved", 60_000);
        approved.signal("approve", "bia");

        assertEquals("timed out", expired.result(TIMEOUT));
        assertEquals("approved by bia", approved.result(TIMEOUT));
    }

    @Test
    void failedStepIsRetriedThenCompensated() throws Exception {
        AtomicInteger shipAttempts = new AtomicInteger();
        AtomicInteger refunds = new AtomicInteger();
        engine.register("order", String.class, String.class, (ctx, input) -> {
            ctx.run("charge", step -> { });
            try {
                ctx.step("ship", String.class, RetryPolicy.fixed(3, Duration.ofMillis(1)), step -> {
                    shipAttempts.incrementAndGet();
                    throw new IllegalStateException("warehouse offline");
                });
                return "shipped";
            } catch (StepFailedException e) {
                ctx.run("refund", step -> refunds.incrementAndGet());
                return "refunded after: " + e.errorType();
            }
        }).startWorkers();

        assertEquals("refunded after: java.lang.IllegalStateException",
                engine.<String>start("order", "order-1", "x").result(TIMEOUT));
        assertEquals(3, shipAttempts.get());
        assertEquals(1, refunds.get());
    }

    @Test
    void uncaughtExceptionFailsTheWorkflow() {
        engine.register("broken", String.class, String.class, (ctx, input) -> {
            throw new IllegalArgumentException("bad input: " + input);
        }).startWorkers();

        WorkflowHandle<String> handle = engine.start("broken", "broken-1", "x");
        WorkflowFailedException failure = assertThrows(WorkflowFailedException.class, () -> handle.result(TIMEOUT));
        assertEquals("java.lang.IllegalArgumentException", failure.errorType());
    }
}
