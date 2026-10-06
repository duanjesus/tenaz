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

    private Lease claimOne(String worker, Instant now) {
        List<Lease> leases = journal.claim(worker, TYPES, TTL, now, 10);
        assertEquals(1, leases.size());
        return leases.get(0);
    }

    private void create(String id) {
        assertTrue(journal.create(id, new Event.WorkflowStarted("t", "null", T0)));
    }

    @Test
    void aLiveLeaseExcludesOtherWorkers() {
        create("wf");
        assertEquals(1, journal.claim("a", TYPES, TTL, T0, 10).size());
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(9), 10).isEmpty());
    }

    @Test
    void takeoverFencesTheWorkerThatLostItsLease() {
        create("wf");
        Lease zombie = claimOne("a", T0);
        Lease successor = claimOne("b", T0.plusSeconds(10));

        assertTrue(successor.epoch() > zombie.epoch());
        assertThrows(FencedException.class,
                () -> journal.append(zombie, 1, List.of(new Event.StepScheduled(0, "s"))));
        assertEquals(Set.of(successor), journal.renew(List.of(zombie, successor), TTL, T0.plusSeconds(11)));
        assertEquals(1, journal.load("wf").orElseThrow().version());

        journal.append(successor, 1, List.of(new Event.StepCompleted(0, "1")));
        assertEquals(List.of(new Event.StepCompleted(0, "1")), journal.loadSince("wf", 1));
        assertEquals(2, journal.load("wf").orElseThrow().version());
    }

    @Test
    void claimTakesAtMostTheRequestedNumber() {
        for (int i = 0; i < 5; i++) {
            create("wf-" + i);
        }
        assertEquals(3, journal.claim("a", TYPES, TTL, T0, 3).size());
        assertEquals(2, journal.claim("b", TYPES, TTL, T0, 3).size());
    }

    @Test
    void decisionsBasedOnAStaleHistoryAreRejected() {
        create("wf");
        Lease lease = claimOne("a", T0);
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertThrows(VersionConflictException.class,
                () -> journal.append(lease, 1, List.of(new Event.StepScheduled(0, "s"))));
    }

    @Test
    void parkedWorkflowBecomesClaimableOnlyWhenSomethingHappens() {
        create("wf");
        Lease lease = claimOne("a", T0);
        journal.append(lease, 1, List.of(new Event.TimerStarted(0, T0.plusSeconds(60))));
        assertTrue(journal.park(lease, 2));

        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(30), 10).isEmpty());
        assertEquals(0, journal.fireDueTimers(T0.plusSeconds(59)));
        assertEquals(1, journal.fireDueTimers(T0.plusSeconds(60)));
        assertEquals(0, journal.fireDueTimers(T0.plusSeconds(61)), "a timer fires once");
        assertEquals(1, journal.claim("b", TYPES, TTL, T0.plusSeconds(61), 10).size());
    }

    @Test
    void parkRefusesWhenAnEventArrivedInTheMeantime() {
        create("wf");
        Lease lease = claimOne("a", T0);
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertFalse(journal.park(lease, 1));
        assertTrue(journal.park(lease, 2));
    }

    @Test
    void finishedWorkflowIgnoresLateEvents() {
        create("wf");
        Lease lease = claimOne("a", T0);
        journal.append(lease, 1, List.of(new Event.WorkflowCompleted("1")));
        journal.appendExternal("wf", new Event.SignalReceived("s", "1"));

        assertEquals(2, journal.load("wf").orElseThrow().version());
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(60), 10).isEmpty());
    }

    @Test
    void deliveriesLandTogetherWithTheOwnersEvents() {
        create("parent");
        Lease parent = claimOne("a", T0);
        assertTrue(journal.createChild(parent, "child", new Event.WorkflowStarted("t", "null", T0, "parent", 0)));
        Lease child = claimOne("a", T0);

        journal.append(child, 1, List.of(new Event.WorkflowCompleted("1")),
                List.of(new Journal.Delivery("parent", new Event.ChildCompleted(0, "1")),
                        new Journal.Delivery("nobody", new Event.ChildCompleted(0, "1"))));

        assertEquals(Journal.WorkflowStatus.COMPLETED, journal.load("child").orElseThrow().status());
        assertEquals(List.of(new Event.ChildCompleted(0, "1")), journal.loadSince("parent", 1));
    }

    @Test
    void onlyTheOwnerOfTheParentMayCreateItsChildren() {
        create("parent");
        Lease zombie = claimOne("a", T0);
        Lease owner = claimOne("b", T0.plusSeconds(10));
        Event.WorkflowStarted start = new Event.WorkflowStarted("t", "null", T0, "parent", 3);

        assertThrows(FencedException.class, () -> journal.createChild(zombie, "child", start));
        assertTrue(journal.createChild(owner, "child", start));
        assertFalse(journal.createChild(owner, "child", start));
        assertEquals(start, journal.started("child").orElseThrow());
        assertTrue(journal.started("nobody").isEmpty());
    }

    @Test
    void aCancelledWorkflowIsOver() {
        create("wf");
        Lease lease = claimOne("a", T0);
        journal.append(lease, 1, List.of(new Event.WorkflowCancelled("stop")));

        assertEquals(Journal.WorkflowStatus.CANCELLED, journal.load("wf").orElseThrow().status());
        assertTrue(journal.claim("b", TYPES, TTL, T0.plusSeconds(60), 10).isEmpty());
    }

    @Test
    void workflowsCanBeListedFilteredAndInspected() {
        create("order-1");
        create("order-2");
        Lease parent = journal.claim("a", TYPES, TTL, T0, 1).get(0);
        journal.createChild(parent, "refund-1", new Event.WorkflowStarted("u", "null", T0, parent.workflowId(), 0));
        journal.append(parent, 1, List.of(new Event.WorkflowCompleted("1")));
        JournalBrowser browser = (JournalBrowser) journal;

        assertEquals(3, browser.list(JournalBrowser.Filter.ALL, 10).size());
        assertEquals(2, browser.list(JournalBrowser.Filter.ALL, 2).size());
        assertEquals(List.of("refund-1"), ids(browser, new JournalBrowser.Filter("u", null, null)));
        assertEquals(List.of(parent.workflowId()),
                ids(browser, new JournalBrowser.Filter(null, Journal.WorkflowStatus.COMPLETED, null)));
        assertEquals(List.of("refund-1"), ids(browser, new JournalBrowser.Filter(null, null, "REFUND")));
        assertEquals(parent.workflowId(), browser.list(new JournalBrowser.Filter("u", null, null), 1).get(0).parentId());
        assertEquals(1L, browser.countByStatus().get(Journal.WorkflowStatus.COMPLETED));
        assertEquals(2L, browser.countByStatus().get(Journal.WorkflowStatus.RUNNING));

        List<JournalBrowser.RecordedEvent> events = browser.events(parent.workflowId());
        assertEquals(List.of(0L, 1L), events.stream().map(JournalBrowser.RecordedEvent::seq).toList());
        assertEquals(new Event.WorkflowCompleted("1"), events.get(1).event());
        assertTrue(events.get(0).recordedAt() != null && !events.get(1).recordedAt().isBefore(events.get(0).recordedAt()));
        assertTrue(browser.events("nobody").isEmpty());
    }

    @Test
    void purgeDeletesOnlyWorkflowsThatEndedBeforeTheCutoff() {
        create("done-1");
        create("done-2");
        create("running");
        for (Lease lease : journal.claim("a", TYPES, TTL, T0, 2)) {
            journal.append(lease, 1, List.of(new Event.WorkflowCompleted("1")));
        }
        Instant past = Instant.now().minusSeconds(60);
        Instant future = Instant.now().plusSeconds(60);

        assertEquals(0, journal.purge(past, 10), "nothing ended that long ago");
        assertEquals(1, journal.purge(future, 1), "the limit is respected");
        assertEquals(1, journal.purge(future, 10));
        assertEquals(0, journal.purge(future, 10), "a running workflow is never purged");
        assertTrue(journal.load("running").isPresent());
        assertTrue(journal.load("done-1").isEmpty());
        assertTrue(journal.loadSince("done-2", 0).isEmpty());

        assertTrue(journal.create("done-1", new Event.WorkflowStarted("t", "null", T0)), "the id is free again");
    }

    private static List<String> ids(JournalBrowser browser, JournalBrowser.Filter filter) {
        return browser.list(filter, 10).stream().map(JournalBrowser.WorkflowSummary::id).toList();
    }
}
