package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.api.RetryPolicy;
import dev.tenaz.api.StepFailedException;
import dev.tenaz.engine.EngineObserver;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import dev.tenaz.journal.Journal.WorkflowStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ObserverTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** Records what the engine reports, as short strings. */
    private static final class Recorder implements EngineObserver {
        final List<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void workflowEnded(String workflowType, WorkflowStatus status) {
            events.add("ended " + workflowType + " " + status);
        }

        @Override
        public void stepAttempted(String workflowType, String stepName, StepOutcome outcome, Duration took) {
            events.add("step " + workflowType + "/" + stepName + " " + outcome);
        }

        @Override
        public void leaseLost(String workflowId) {
            events.add("lost " + workflowId);
        }

        @Override
        public void leasesRenewed(int count, Duration took) {
            events.add("renewed");
        }

        @Override
        public void journalAppended(int count, Duration took) {
            events.add("appended");
        }

        @Override
        public void workflowsClaimed(int count) {
            throw new IllegalStateException("an observer that fails must not hurt the engine");
        }

        long count(String event) {
            return events.stream().filter(event::equals).count();
        }
    }

    private final Journal journal = new InMemoryJournal();
    private final Recorder recorder = new Recorder();
    private final TenazEngine engine = TenazEngine.builder(journal)
            .pollInterval(Duration.ofMillis(5))
            .leaseTtl(Duration.ofMillis(300))
            .observer(recorder)
            .build();

    @AfterEach
    void stop() {
        engine.close();
    }

    @Test
    void stepsAndEndingsAreReportedWithTheirOutcomes() throws Exception {
        RetryPolicy quick = RetryPolicy.fixed(2, Duration.ofMillis(1));
        engine.register("report", String.class, String.class, (ctx, input) -> {
            ctx.step("fine", String.class, step -> "ok");
            try {
                ctx.step("flaky", String.class, quick, step -> {
                    throw new IllegalStateException("no");
                });
            } catch (StepFailedException e) {
                // expected
            }
            try {
                ctx.step("slow", String.class, RetryPolicy.none().withTimeout(Duration.ofMillis(50)), step -> {
                    Thread.sleep(60_000);
                    return "never";
                });
            } catch (StepFailedException e) {
                // expected
            }
            return "done";
        }).startWorkers();

        assertEquals("done", engine.<String>start("report", "report-1", "x").result(TIMEOUT));

        assertEquals(1, recorder.count("step report/fine COMPLETED"));
        assertEquals(1, recorder.count("step report/flaky RETRIED"));
        assertEquals(1, recorder.count("step report/flaky FAILED"));
        assertEquals(1, recorder.count("step report/slow TIMED_OUT"));
        // The ending is reported just after it is journaled, which is when result() returns.
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (recorder.count("ended report COMPLETED") == 0 && Instant.now().isBefore(deadline)) {
            Thread.sleep(10);
        }
        assertEquals(1, recorder.count("ended report COMPLETED"));
        assertTrue(recorder.count("appended") >= 4);
        assertEquals(0, recorder.events.stream().filter(e -> e.startsWith("lost")).count(),
                "a workflow that ends or parks normally loses no lease");
    }

    @Test
    void aWorkflowTakenByAnotherOwnerIsReportedAsALostLease() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        engine.register("held", String.class, String.class, (ctx, input) -> ctx.step("wait", String.class, step -> {
            started.countDown();
            Thread.sleep(60_000);
            return "never";
        })).startWorkers();
        engine.start("held", "held-1", "x");
        assertTrue(started.await(5, TimeUnit.SECONDS));

        // Someone claims the workflow as if this engine's lease had expired long ago.
        assertEquals(1, journal.claim("thief", Set.of("held"), Duration.ofHours(1),
                Instant.now().plusSeconds(3600), 10).size());

        Instant deadline = Instant.now().plus(TIMEOUT);
        while (recorder.count("lost held-1") == 0 && Instant.now().isBefore(deadline)) {
            Thread.sleep(20);
        }
        assertEquals(1, recorder.count("lost held-1"));
        assertTrue(recorder.count("renewed") >= 1);
        assertTrue(engine.lastLeaseRenewal().isPresent());
    }
}
