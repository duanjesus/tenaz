package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RetentionTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    protected final Journal journal = createJournal();

    protected Journal createJournal() {
        return new InMemoryJournal();
    }

    private final TenazEngine engine = TenazEngine.builder(journal)
            .pollInterval(Duration.ofMillis(5))
            .retention(Duration.ofMillis(300))
            .build();

    @AfterEach
    void stop() {
        engine.close();
    }

    @Test
    void endedWorkflowsAreDeletedAfterTheirRetentionAndRunningOnesAreKept() throws Exception {
        engine.register("quick", String.class, String.class, (ctx, input) -> ctx.step("echo", String.class, s -> input))
                .register("sleeper", String.class, String.class, (ctx, input) -> {
                    ctx.sleep(Duration.ofHours(1));
                    return "woke";
                })
                .startWorkers();
        WorkflowHandle<String> quick = engine.start("quick", "quick-1", "hi");
        WorkflowHandle<String> sleeper = engine.start("sleeper", "sleeper-1", "x");
        assertEquals("hi", quick.result(TIMEOUT));

        Instant deadline = Instant.now().plus(TIMEOUT);
        while (journal.load("quick-1").isPresent() && Instant.now().isBefore(deadline)) {
            Thread.sleep(20);
        }

        assertThrows(IllegalArgumentException.class, quick::isDone, "the ended workflow is gone");
        assertFalse(sleeper.isDone(), "the running workflow is untouched");

        // Gone means gone: the id starts a new execution.
        assertEquals("again", engine.<String>start("quick", "quick-1", "again").result(TIMEOUT));
    }
}
