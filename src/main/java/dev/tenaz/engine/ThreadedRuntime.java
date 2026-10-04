package dev.tenaz.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * The production runtime: one virtual thread per task, wall-clock time.
 *
 * <p>Engine tasks and step bodies run on separate executors because they stop differently. A
 * step body is user code that may block for as long as it likes, so shutdown interrupts it. An
 * engine task is short and may be in the middle of a journal call, so shutdown lets it finish:
 * interrupting it would abandon a statement that the database keeps executing.
 */
final class ThreadedRuntime implements EngineRuntime {

    private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService steps = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("tenaz-timer").factory());

    @Override
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Override
    public void execute(Runnable task) {
        try {
            tasks.execute(task);
        } catch (RejectedExecutionException e) {
            // shut down
        }
    }

    @Override
    public void schedule(Duration delay, Runnable task) {
        try {
            timer.schedule(() -> execute(task), delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            // shut down
        }
    }

    @Override
    public <T> Runnable call(Callable<T> body, BiConsumer<T, Throwable> then) {
        try {
            Future<?> running = steps.submit(() -> {
                T result;
                try {
                    result = body.call();
                } catch (Throwable e) {
                    then.accept(null, e);
                    return;
                }
                then.accept(result, null);
            });
            return () -> running.cancel(true);
        } catch (RejectedExecutionException e) {
            return () -> { };
        }
    }

    @Override
    public void shutdown(boolean awaitTermination) {
        timer.shutdownNow();
        steps.shutdownNow();
        tasks.shutdown();
        if (awaitTermination) {
            try {
                tasks.awaitTermination(10, TimeUnit.SECONDS);
                steps.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
