package dev.tenaz.api;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/** A reference to one workflow execution, usable from any process that shares the journal. */
public interface WorkflowHandle<O> {

    String workflowId();

    /**
     * Waits for the workflow to finish.
     *
     * @throws WorkflowFailedException if it ended in failure
     * @throws TimeoutException        if it is still running after {@code timeout}
     */
    O result(Duration timeout) throws TimeoutException, InterruptedException;

    boolean isDone();

    void signal(String name, Object payload);
}
