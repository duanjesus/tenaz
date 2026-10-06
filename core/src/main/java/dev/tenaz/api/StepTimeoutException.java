package dev.tenaz.api;

import java.time.Duration;

/** An attempt at a step ran longer than its {@link RetryPolicy#attemptTimeout()} allows. */
public class StepTimeoutException extends RuntimeException {

    public StepTimeoutException(String stepName, Duration timeout) {
        super("step '" + stepName + "' did not finish within " + timeout);
    }
}
