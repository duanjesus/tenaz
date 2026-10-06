package dev.tenaz.api;

/** Thrown to whoever asks for the result of a workflow that ended in failure. */
public class WorkflowFailedException extends RuntimeException {

    private final String errorType;

    public WorkflowFailedException(String workflowId, String errorType, String message) {
        super("workflow '" + workflowId + "' failed: " + errorType + ": " + message);
        this.errorType = errorType;
    }

    public String errorType() {
        return errorType;
    }
}
