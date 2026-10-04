package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CrashRecoveryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    protected final Journal journal = createJournal();

    protected Journal createJournal() {
        return new InMemoryJournal();
    }

    private TenazEngine engine(String name, Workflow<String, String> workflow) {
        return TenazEngine.builder(journal)
                .workerId(name)
                .leaseTtl(Duration.ofMillis(150))
                .pollInterval(Duration.ofMillis(5))
                .build()
                .register("wf", String.class, String.class, workflow);
    }

    @Test
    void anotherEngineResumesFromTheLastJournaledStep() throws Exception {
        AtomicInteger reserved = new AtomicInteger();
        AtomicInteger charged = new AtomicInteger();
        Set<String> chargeKeys = ConcurrentHashMap.newKeySet();
        CountDownLatch chargeStarted = new CountDownLatch(1);
        CountDownLatch paymentGatewayUp = new CountDownLatch(1);
        Workflow<String, String> workflow = (ctx, input) -> {
            String reservation = ctx.step("reserve", String.class, step -> "r" + reserved.incrementAndGet());
            return ctx.step("charge", String.class, step -> {
                charged.incrementAndGet();
                chargeKeys.add(step.idempotencyKey());
                chargeStarted.countDown();
                paymentGatewayUp.await();
                return reservation + " charged";
            });
        };

        TenazEngine first = engine("first", workflow).startWorkers();
        WorkflowHandle<String> handle = first.start("wf", "order-1", "x");
        assertTrue(chargeStarted.await(5, TimeUnit.SECONDS));
        first.crash();

        try (TenazEngine second = engine("second", workflow).startWorkers()) {
            paymentGatewayUp.countDown();
            assertEquals("r1 charged", handle.result(TIMEOUT));
        }
        assertEquals(1, reserved.get(), "a journaled step is never run again");
        assertEquals(2, charged.get(), "the step that was mid-flight is run again");
        assertEquals(1, chargeKeys.size(), "and it carries the same idempotency key");
    }

    @Test
    void sleepingWorkflowOutlivesTheEngineThatStartedIt() throws Exception {
        Workflow<String, String> workflow = (ctx, input) -> {
            ctx.sleep(Duration.ofMillis(300));
            return "woke up";
        };

        TenazEngine first = engine("first", workflow).startWorkers();
        WorkflowHandle<String> handle = first.start("wf", "sleeper-1", "x");
        Thread.sleep(100);
        first.crash();

        try (TenazEngine second = engine("second", workflow).startWorkers()) {
            assertEquals("woke up", handle.result(TIMEOUT));
        }
    }

    @Test
    void gracefulShutdownHandsWorkOverWithoutWaitingForTheLease() throws Exception {
        CountDownLatch stepStarted = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        Workflow<String, String> workflow = (ctx, input) -> ctx.step("slow", String.class, step -> {
            stepStarted.countDown();
            if (attempts.incrementAndGet() == 1) {
                Thread.sleep(60_000);
            }
            return "done";
        });

        TenazEngine first = TenazEngine.builder(journal).leaseTtl(Duration.ofMinutes(10))
                .pollInterval(Duration.ofMillis(5)).build()
                .register("wf", String.class, String.class, workflow).startWorkers();
        WorkflowHandle<String> handle = first.start("wf", "handover-1", "x");
        assertTrue(stepStarted.await(5, TimeUnit.SECONDS));
        first.close();

        try (TenazEngine second = engine("second", workflow).startWorkers()) {
            assertEquals("done", handle.result(TIMEOUT));
        }
    }
}
