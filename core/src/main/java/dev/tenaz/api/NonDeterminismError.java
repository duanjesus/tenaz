package dev.tenaz.api;

/**
 * The workflow code no longer matches its own history: it asked for something different from
 * what it asked for at the same point in an earlier run.
 */
public class NonDeterminismError extends Error {

    public NonDeterminismError(String message) {
        super(message);
    }
}
