package dev.tenaz.api;

/**
 * Cancellation was requested for the workflow.
 *
 * <p>Inside workflow code it is thrown once, at the first point where the code waits for
 * something that had not happened when cancellation was requested, or tries to start something
 * new. The code may catch it to undo what it did, and may keep using the context while it does;
 * rethrowing it ends the workflow as cancelled. A step that was already running is not
 * interrupted: wait on its promise again to learn how it ended.
 *
 * <p>Outside, it is thrown to whoever asks for the result of a workflow that ended cancelled.
 */
public class WorkflowCancelledException extends RuntimeException {

    private final String reason;

    public WorkflowCancelledException(String reason) {
        super("workflow cancelled: " + reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
