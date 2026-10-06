package dev.tenaz.api;

/** A child workflow ended in failure or was cancelled. The parent may catch this and carry on. */
public class ChildWorkflowFailedException extends RuntimeException {

    private final String childId;
    private final String errorType;

    public ChildWorkflowFailedException(String childId, String errorType, String message) {
        super("child workflow '" + childId + "' failed: " + errorType + ": " + message);
        this.childId = childId;
        this.errorType = errorType;
    }

    public String childId() {
        return childId;
    }

    public String errorType() {
        return errorType;
    }

    public boolean cancelled() {
        return WorkflowCancelledException.class.getName().equals(errorType);
    }
}
