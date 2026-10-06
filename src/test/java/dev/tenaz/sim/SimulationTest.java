package dev.tenaz.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.engine.TenazEngine;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Deterministic simulation of the whole cluster. Each seed is a different history of crashes,
 * freezes, clock skew and journal failures, and a failing seed fails the same way every time.
 *
 * <p>{@code -Dtenaz.sim.seeds=N} runs more seeds; {@code -Dtenaz.sim.seed=S} replays one.
 */
class SimulationTest {

    // The engine logs every injected failure, which is thousands of lines of expected noise.
    private static final Logger ENGINE_LOG = Logger.getLogger(TenazEngine.class.getName());

    @BeforeAll
    static void quiet() {
        ENGINE_LOG.setLevel(Level.OFF);
    }

    @AfterAll
    static void restore() {
        ENGINE_LOG.setLevel(null);
    }

    @Test
    void everySeedConvergesWithEveryEffectAppliedExactlyOnce() {
        Long single = Long.getLong("tenaz.sim.seed");
        long first = single != null ? single : 0;
        long count = single != null ? 1 : Long.getLong("tenaz.sim.seeds", 500);

        long events = 0;
        long crashes = 0;
        long pauses = 0;
        long failures = 0;
        long repeated = 0;
        long simulatedSeconds = 0;
        long cancelled = 0;
        long started = System.nanoTime();
        for (long seed = first; seed < first + count; seed++) {
            Simulation.Report report = Simulation.run(seed);
            events += report.events();
            crashes += report.crashes();
            pauses += report.pauses();
            failures += report.journalFailures();
            repeated += report.repeatedExecutions();
            simulatedSeconds += report.simulatedTime().toSeconds();
            cancelled += report.cancelled();
        }
        System.out.printf("simulation: %d seeds, %d events, %d h of simulated time in %d s; injected %d crashes, "
                        + "%d freezes, %d journal failures; %d step executions repeated; %d workflows cancelled%n",
                count, events, simulatedSeconds / 3600, (System.nanoTime() - started) / 1_000_000_000,
                crashes, pauses, failures, repeated, cancelled);
        assertTrue(single != null || repeated > 0, "the faults must have bitten, or this test proves nothing");
    }

    @Test
    void theSameSeedProducesTheSameRun() {
        Simulation.Report first = Simulation.run(1234);
        Simulation.Report second = Simulation.run(1234);
        Simulation.Report other = Simulation.run(1235);

        assertEquals(first, second);
        assertNotEquals(first.fingerprint(), other.fingerprint());
    }

    /** The simulator has to be able to fail: remove fencing from the journal and it must notice. */
    @Test
    void aJournalWithoutFencingIsCaught() {
        AssertionError violation = null;
        long seed = 0;
        while (violation == null && seed < 5_000) {
            try {
                Simulation.runWithoutFencing(seed);
                seed++;
            } catch (AssertionError e) {
                violation = e;
            }
        }
        assertTrue(violation != null, "5000 seeds ran against a journal without fencing and none failed");
        System.out.println("simulation: journal without fencing caught at " + violation.getMessage());
    }
}
