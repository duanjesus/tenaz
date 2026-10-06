package dev.tenaz.journal;

import java.time.Instant;

/**
 * One entry in a workflow's history. The history is append-only and is the single source of
 * truth: a workflow's state is whatever replaying its code against these events produces.
 *
 * <p>Commands ({@code StepScheduled}, {@code TimerStarted}, {@code SideEffectRecorded},
 * {@code ChildStarted}) carry the {@code seq} the workflow code assigned to them, which is how a
 * replay matches code to history.
 */
public sealed interface Event {

    /**
     * @param parentId  the workflow that started this one as a child, or null
     * @param parentSeq the parent's command that started it, or null
     */
    record WorkflowStarted(String workflowType, String input, Instant startedAt, String parentId,
                           Integer parentSeq) implements Event {

        public WorkflowStarted(String workflowType, String input, Instant startedAt) {
            this(workflowType, input, startedAt, null, null);
        }
    }

    record StepScheduled(int seq, String name) implements Event {}

    record StepCompleted(int seq, String result) implements Event {}

    record StepFailed(int seq, String errorType, String message) implements Event {}

    record TimerStarted(int seq, Instant fireAt) implements Event {}

    record TimerFired(int seq) implements Event {}

    record SideEffectRecorded(int seq, String value) implements Event {}

    record ChildStarted(int seq, String workflowType, String childId, String input) implements Event {}

    record ChildCompleted(int seq, String result) implements Event {}

    record ChildFailed(int seq, String errorType, String message) implements Event {}

    record SignalReceived(String name, String payload) implements Event {}

    record CancelRequested(String reason) implements Event {}

    /** Which version of a changed piece of code this execution runs; see {@code WorkflowContext.version}. */
    record VersionMarked(String changeId, int version) implements Event {}

    record WorkflowCompleted(String result) implements Event {}

    record WorkflowFailed(String errorType, String message) implements Event {}

    record WorkflowCancelled(String reason) implements Event {}

    default boolean endsWorkflow() {
        return this instanceof WorkflowCompleted || this instanceof WorkflowFailed
                || this instanceof WorkflowCancelled;
    }
}
