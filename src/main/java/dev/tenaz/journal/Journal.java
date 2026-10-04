package dev.tenaz.journal;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Durable storage for workflow histories plus the coordination primitives workers need:
 * leases with fencing epochs, optimistic appends and timers.
 *
 * <p>Every method is atomic. Time is always passed in by the caller so that implementations
 * never read a clock of their own.
 */
public interface Journal {

    /** Creates a workflow. Returns false, changing nothing, if the id already exists. */
    boolean create(String workflowId, Event.WorkflowStarted started);

    Optional<History> load(String workflowId);

    /** The events from position {@code fromVersion} on; empty if there are none yet. */
    List<Event> loadSince(String workflowId, long fromVersion);

    /**
     * Claims up to {@code limit} workflows of the given types that need work: they have events
     * no owner has processed, or their owner's lease expired. Each claim bumps the workflow's
     * epoch, which fences every earlier owner.
     */
    List<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now, int limit);

    /** Extends the leases that are still held and returns them; the rest have been fenced. */
    Set<Lease> renew(Collection<Lease> leases, Duration ttl, Instant now);

    /**
     * Appends on behalf of the workflow's owner: step outcomes and the commands a replay
     * produced. Fails with {@link FencedException} if the lease was lost, and with
     * {@link VersionConflictException} if the history moved since it was read, in which case the
     * caller must read the new events and replay.
     */
    void append(Lease lease, long expectedVersion, List<Event> events);

    /** Appends an event that comes from outside the workflow, such as a signal. */
    void appendExternal(String workflowId, Event event);

    /** Appends {@code TimerFired} for every timer due at {@code now}. Returns how many fired. */
    int fireDueTimers(Instant now);

    /**
     * Releases a workflow that is waiting only on timers or signals. Returns false, keeping the
     * lease, if the history moved past {@code version} in the meantime.
     */
    boolean park(Lease lease, long version);

    /** Gives a lease up early so that another worker can claim the workflow immediately. */
    void abandon(Lease lease);

    /**
     * Registers a listener that is told when a workflow's history grows. Notifications are
     * best-effort: they may be lost, repeated or late, and exist only to save callers from
     * waiting for their next poll. The listener must not block. Returns a handle that unsubscribes.
     */
    Runnable subscribe(ChangeListener listener);

    /** Blocks until the history is longer than {@code version}. Returns false on timeout. */
    boolean awaitChange(String workflowId, long version, Duration timeout) throws InterruptedException;

    @FunctionalInterface
    interface ChangeListener {
        /**
         * @param version   the length of the history after the change
         * @param claimable whether the workflow has no owner, so that someone should claim it
         */
        void changed(String workflowId, long version, boolean claimable);
    }

    record History(String workflowType, List<Event> events, WorkflowStatus status) {
        public long version() {
            return events.size();
        }
    }

    record Lease(String workflowId, String workerId, long epoch) {}

    enum WorkflowStatus { RUNNING, COMPLETED, FAILED }

    /** The lease was taken over by another worker; the holder must stop working on the workflow. */
    final class FencedException extends RuntimeException {
        public FencedException(Lease lease) {
            super("lease fenced: " + lease);
        }
    }

    final class VersionConflictException extends RuntimeException {
        public VersionConflictException(String workflowId, long expected, long actual) {
            super("workflow " + workflowId + " expected version " + expected + " but was " + actual);
        }
    }
}
