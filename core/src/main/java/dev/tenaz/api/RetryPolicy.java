package dev.tenaz.api;

import java.time.Duration;

public record RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {

    public static final RetryPolicy DEFAULT =
            new RetryPolicy(3, Duration.ofMillis(100), 2.0, Duration.ofSeconds(10));

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, 1.0, Duration.ZERO);
    }

    public static RetryPolicy fixed(int maxAttempts, Duration backoff) {
        return new RetryPolicy(maxAttempts, backoff, 1.0, backoff);
    }

    public Duration next(Duration backoff) {
        Duration grown = Duration.ofNanos((long) (backoff.toNanos() * multiplier));
        return grown.compareTo(maxBackoff) > 0 ? maxBackoff : grown;
    }
}
