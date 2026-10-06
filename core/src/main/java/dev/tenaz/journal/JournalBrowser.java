package dev.tenaz.journal;

import dev.tenaz.journal.Journal.WorkflowStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Read-only access to a journal for people rather than for engines: listing workflows and
 * looking at what happened in one.
 *
 * <p>The times reported here are when the journal recorded each event, by the journal's own
 * clock. They are for display. Nothing an engine decides depends on them.
 */
public interface JournalBrowser {

    /**
     * @param type       only workflows of this type, or null for any
     * @param status     only workflows in this status, or null for any
     * @param idContains only workflows whose id contains this text, or null for any
     */
    record Filter(String type, WorkflowStatus status, String idContains) {
        public static final Filter ALL = new Filter(null, null, null);
    }

    record WorkflowSummary(String id, String type, WorkflowStatus status, long events, Instant startedAt,
                           Instant lastEventAt, String parentId) {}

    record RecordedEvent(long seq, Instant recordedAt, Event event) {}

    /** The most recently started workflows that match, newest first. */
    List<WorkflowSummary> list(Filter filter, int limit);

    /** How many workflows there are in each status. */
    Map<WorkflowStatus, Long> countByStatus();

    /** A workflow's history with the time each event was recorded; empty if there is no such workflow. */
    List<RecordedEvent> events(String workflowId);
}
