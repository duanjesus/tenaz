package dev.tenaz.journal;

import java.time.Instant;

/**
 * One entry in a workflow's history. The history is append-only and is the single source of
 * truth: a workflow's state is whatever replaying its code against these events produces.
 *
 * <p>Commands ({@code StepScheduled}, {@code TimerStarted}, {@code SideEffectRecorded}) carry the
 * {@code seq} the workflow code assigned to them, which is how a replay matches code to history.
 */
public sealed interface Event {

    record WorkflowStarted(String workflowType, String input, Instant startedAt) implements Event {}

    record StepScheduled(int seq, String name) implements Event {}

    record StepCompleted(int seq, String result) implements Event {}

    record StepFailed(int seq, String errorType, String message) implements Event {}

    record TimerStarted(int seq, Instant fireAt) implements Event {}

    record TimerFired(int seq) implements Event {}

    record SideEffectRecorded(int seq, String value) implements Event {}

    record SignalReceived(String name, String payload) implements Event {}

    record WorkflowCompleted(String result) implements Event {}

    record WorkflowFailed(String errorType, String message) implements Event {}
}
