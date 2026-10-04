package dev.tenaz.api;

/** A step ran out of retries. Workflow code may catch this to run compensations. */
public class StepFailedException extends RuntimeException {

    private final String stepName;
    private final String errorType;

    public StepFailedException(String stepName, String errorType, String message) {
        super("step '" + stepName + "' failed: " + errorType + ": " + message);
        this.stepName = stepName;
        this.errorType = errorType;
    }

    public String stepName() {
        return stepName;
    }

    public String errorType() {
        return errorType;
    }
}
