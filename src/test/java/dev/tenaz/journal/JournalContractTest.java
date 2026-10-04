package dev.tenaz.journal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.journal.Journal.FencedException;
import dev.tenaz.journal.Journal.Lease;
import dev.tenaz.journal.Journal.VersionConflictException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

abstract class JournalContractTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Set<String> TYPES = Set.of("t");

    protected final Journal journal = createJournal();

    protected abstract Journal createJournal();

    private void create(String id) {
        assertTrue(journal.create(id, new Event.WorkflowStarted("t", "null", T0)));
    }

    @Test
    void aLiveLeaseExcludesOtherWorkers() {
        create("wf");
        assertTrue(journal.claim("a", TYPES, TTL, T0).isPresent());
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(9)).isEmpty());
    }

    @Test
    void takeoverFencesTheWorkerThatLostItsLease() {
        create("wf");
        Lease zombie = journal.claim("a", TYPES, TTL, T0).orElseThrow();
        Lease successor = journal.claim("b", TYPES, TTL, T0.plusSeconds(10)).orElseThrow();

        assertTrue(successor.epoch() > zombie.epoch());
        assertThrows(FencedException.class,
                () -> journal.appendStepResult(zombie, new Event.StepCompleted(0, "1")));
        assertThrows(FencedException.class,
                () -> journal.appendDecisions(zombie, 1, List.of(new Event.StepScheduled(0, "s"))));
        assertFalse(journal.renew(zombie, TTL, T0.plusSeconds(11)));
        assertEquals(1, journal.load("wf").orElseThrow().version());

        journal.appendStepResult(successor, new Event.StepCompleted(0, "1"));
        assertEquals(2, journal.load("wf").orElseThrow().version());
    }

    @Test
    void decisionsBasedOnAStaleHistoryAreRejected() {
        create("wf");
        Lease lease = journal.claim("a", TYPES, TTL, T0).orElseThrow();
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertThrows(VersionConflictException.class,
                () -> journal.appendDecisions(lease, 1, List.of(new Event.StepScheduled(0, "s"))));
    }

    @Test
    void parkedWorkflowBecomesClaimableOnlyWhenSomethingHappens() {
        create("wf");
        Lease lease = journal.claim("a", TYPES, TTL, T0).orElseThrow();
        journal.appendDecisions(lease, 1, List.of(new Event.TimerStarted(0, T0.plusSeconds(60))));
        assertTrue(journal.park(lease, 2));

        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(30)).isEmpty());
        assertEquals(0, journal.fireDueTimers(T0.plusSeconds(59)));
        assertEquals(1, journal.fireDueTimers(T0.plusSeconds(60)));
        assertEquals(0, journal.fireDueTimers(T0.plusSeconds(61)), "a timer fires once");
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(61)).isPresent());
    }

    @Test
    void parkRefusesWhenAnEventArrivedInTheMeantime() {
        create("wf");
        Lease lease = journal.claim("a", TYPES, TTL, T0).orElseThrow();
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertFalse(journal.park(lease, 1));
        assertTrue(journal.park(lease, 2));
    }

    @Test
    void finishedWorkflowIgnoresLateEvents() {
        create("wf");
        Lease lease = journal.claim("a", TYPES, TTL, T0).orElseThrow();
        journal.appendDecisions(lease, 1, List.of(new Event.WorkflowCompleted("1")));
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertEquals(2, journal.load("wf").orElseThrow().version());
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(60)).isEmpty());
    }
}
