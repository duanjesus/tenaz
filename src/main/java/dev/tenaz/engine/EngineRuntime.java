package dev.tenaz.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Everything the engine needs from the world besides the journal: time, a way to run tasks, and
 * a way to run step bodies. The engine itself never blocks, sleeps or reads a clock, so swapping
 * this interface swaps reality: threads and wall-clock time in production, or a single-threaded
 * seeded event loop under simulation.
 */
public interface EngineRuntime {

    Clock clock();

    /** Runs a task soon. Tasks may run concurrently with each other. */
    void execute(Runnable task);

    void schedule(Duration delay, Runnable task);

    /**
     * Runs a step body, which is allowed to block, and passes its result or its failure to
     * {@code then}. Returns a handle that cancels the call if it has not finished.
     */
    <T> Runnable call(Callable<T> body, BiConsumer<T, Throwable> then);

    /** Stops running tasks and interrupts step bodies. */
    void shutdown(boolean awaitTermination);
}
