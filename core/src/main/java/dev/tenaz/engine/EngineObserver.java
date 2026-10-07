package dev.tenaz.engine;

import dev.tenaz.journal.Journal.WorkflowStatus;
import java.time.Duration;

/**
 * Told what an engine is doing, for metrics and logs. Every method does nothing by default, so
 * an observer implements only what it cares about. An observer cannot affect the engine: what
 * it throws is swallowed.
 */
public interface EngineObserver {

    enum StepOutcome {
        COMPLETED,
        /** Failed with attempts left; it will be tried again. */
        RETRIED,
        /** Failed for good: out of attempts, or the failure was not retryable. */
        FAILED,
        /** Ran past its timeout and was interrupted; whether it is retried depends on the policy. */
        TIMED_OUT
    }

    /** The engine took ownership of this many workflows in one claim. */
    default void workflowsClaimed(int count) {}

    /** A workflow this engine was driving ended. */
    default void workflowEnded(String workflowType, WorkflowStatus status) {}

    /** One attempt at a step finished, however it finished. */
    default void stepAttempted(String workflowType, String stepName, StepOutcome outcome, Duration took) {}

    /**
     * The engine found that a workflow it was driving now belongs to another engine. Expected
     * after a pause or a partition; a steady trickle with neither means leases are expiring
     * under a healthy engine, which makes steps run more than once for no reason.
     */
    default void leaseLost(String workflowId) {}

    /** The engine renewed its leases. */
    default void leasesRenewed(int count, Duration took) {}

    /** The engine wrote to a workflow's history. */
    default void journalAppended(int events, Duration took) {}
}
