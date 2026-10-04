package dev.tenaz.api;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

/** The only door between workflow code and the non-deterministic world. */
public interface WorkflowContext {

    String workflowId();

    /**
     * Runs a step and returns its result. The result is journaled, so the step's body is not run
     * again once it has completed, no matter how often the workflow is replayed. A step may run
     * more than once if a worker dies before journaling it: make the body idempotent using
     * {@link StepContext#idempotencyKey()}.
     *
     * @throws StepFailedException once the retry policy is exhausted
     */
    <T> T step(String name, Class<T> type, StepFunction<T> body);

    <T> T step(String name, Class<T> type, RetryPolicy retry, StepFunction<T> body);

    /** A step with no result. */
    void run(String name, StepAction body);

    /** Schedules a step without waiting for it, so that several can run in parallel. */
    <T> DurablePromise<T> stepAsync(String name, Class<T> type, StepFunction<T> body);

    <T> DurablePromise<T> stepAsync(String name, Class<T> type, RetryPolicy retry, StepFunction<T> body);

    /** Durable sleep: holds no thread, and survives restarts however long the duration is. */
    void sleep(Duration duration);

    DurablePromise<Void> timer(Duration duration);

    /** Waits for the next signal with this name that the workflow has not consumed yet. */
    <T> T awaitSignal(String name, Class<T> type);

    <T> DurablePromise<T> signal(String name, Class<T> type);

    /** Waits until one of the promises resolves and returns the one that resolved first. */
    DurablePromise<?> anyOf(DurablePromise<?>... promises);

    /** Records the value of a cheap non-deterministic expression so that replays see the same one. */
    <T> T sideEffect(Class<T> type, Supplier<T> supplier);

    Instant now();

    UUID randomUUID();
}
