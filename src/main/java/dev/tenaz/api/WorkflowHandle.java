package dev.tenaz.api;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/** A reference to one workflow execution, usable from any process that shares the journal. */
public interface WorkflowHandle<O> {

    String workflowId();

    /**
     * Waits for the workflow to finish.
     *
     * @throws WorkflowFailedException    if it ended in failure
     * @throws WorkflowCancelledException if it ended cancelled
     * @throws TimeoutException        if it is still running after {@code timeout}
     */
    O result(Duration timeout) throws TimeoutException, InterruptedException;

    boolean isDone();

    void signal(String name, Object payload);

    /**
     * Asks the workflow to stop. The workflow's code decides what that means: it sees a
     * {@link WorkflowCancelledException} and may clean up before ending.
     */
    void cancel(String reason);
}
