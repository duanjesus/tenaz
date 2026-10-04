package dev.tenaz.engine;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/** The production runtime: one virtual thread per task, wall-clock time. */
final class ThreadedRuntime implements EngineRuntime {

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @Override
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Override
    public void execute(Runnable task) {
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            // shut down
        }
    }

    @Override
    public void schedule(Duration delay, Runnable task) {
        execute(() -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                return;
            }
            task.run();
        });
    }

    @Override
    public <T> Runnable call(Callable<T> body, BiConsumer<T, Throwable> then) {
        try {
            Future<?> running = executor.submit(() -> {
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
        executor.shutdownNow();
        if (awaitTermination) {
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
