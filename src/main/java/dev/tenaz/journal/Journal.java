package dev.tenaz.journal;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

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

    /**
     * Claims one workflow of the given types that needs work: it has events its last owner never
     * processed, or its owner's lease expired. Each claim bumps the workflow's epoch, which
     * fences every earlier owner.
     */
    Optional<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now);

    /** Extends a lease. Returns false if the lease has been fenced. */
    boolean renew(Lease lease, Duration ttl, Instant now);

    /**
     * Appends the commands a replay produced. Fails with {@link VersionConflictException} if the
     * history moved since it was loaded, in which case the caller must replay again.
     */
    void appendDecisions(Lease lease, long expectedVersion, List<Event> events);

    /** Appends a step outcome. Fenced by epoch but not by version: it cannot depend on ordering. */
    void appendStepResult(Lease lease, Event event);

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
     * Registers a listener that is told the id of a workflow whose history grew. Notifications
     * are best-effort: they may be lost, repeated or late, and exist only to save callers from
     * waiting for their next poll. The listener must not block. Returns a handle that unsubscribes.
     */
    Runnable subscribe(Consumer<String> listener);

    /** Blocks until the history is longer than {@code version}. Returns false on timeout. */
    boolean awaitChange(String workflowId, long version, Duration timeout) throws InterruptedException;

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
