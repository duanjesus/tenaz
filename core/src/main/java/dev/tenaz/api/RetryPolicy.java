package dev.tenaz.api;

import java.time.Duration;

/**
 * How a step is retried, and how long each try may take.
 *
 * @param attemptTimeout how long one attempt may run before it is interrupted and counted as a
 *                       failure, or null for no limit
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff,
                          Duration attemptTimeout) {

    public static final RetryPolicy DEFAULT =
            new RetryPolicy(3, Duration.ofMillis(100), 2.0, Duration.ofSeconds(10));

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
    }

    public RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {
        this(maxAttempts, initialBackoff, multiplier, maxBackoff, null);
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    public static RetryPolicy fixed(int maxAttempts, Duration backoff) {
        return new RetryPolicy(maxAttempts, backoff, 1.0, backoff);
    }

    /**
     * The same policy with a limit on each attempt. An attempt that runs past it is interrupted
     * and fails with {@link StepTimeoutException}, to be retried like any other failure.
     * Interrupting does not undo: what the attempt already did in the outside world stays done.
     */
    public RetryPolicy withTimeout(Duration attemptTimeout) {
        return new RetryPolicy(maxAttempts, initialBackoff, multiplier, maxBackoff, attemptTimeout);
    }

    public Duration next(Duration backoff) {
        Duration grown = Duration.ofNanos((long) (backoff.toNanos() * multiplier));
        return grown.compareTo(maxBackoff) > 0 ? maxBackoff : grown;
    }
}
