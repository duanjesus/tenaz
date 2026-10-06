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

    /**
     * Starts another workflow as a child and returns its result. The child is an ordinary
     * workflow with its own history; the parent holds nothing while it waits. Cancelling the
     * parent cancels the children it is still waiting for.
     *
     * @throws ChildWorkflowFailedException if the child fails, is cancelled, or the id is taken
     *                                      by a workflow that is not this parent's child
     */
    <T> T child(String workflowType, String childId, Object input, Class<T> type);

    /** Starts a child without waiting for it, so that several can run at once. */
    <T> DurablePromise<T> childAsync(String workflowType, String childId, Object input, Class<T> type);

    /**
     * Lets workflow code change while executions of the old code are still in flight. Wrap the
     * change in a check of the returned version: an execution that already ran through this
     * point before the change existed gets 0 and must follow the original path; any other gets
     * {@code maxSupported}, recorded so that it gets the same answer on every replay.
     *
     * <pre>{@code
     * if (ctx.version("add-fraud-check", 1) >= 1) {
     *     ctx.run("fraud-check", step -> fraud.check(order));
     * }
     * }</pre>
     */
    int version(String changeId, int maxSupported);

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
