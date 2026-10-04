package dev.tenaz.engine;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A task that never runs concurrently with itself. Any number of requests made while it is
 * running collapse into exactly one more run afterwards, so no request is ever lost.
 */
final class SerialTask {

    private final EngineRuntime runtime;
    private final Runnable body;
    private final AtomicInteger requests = new AtomicInteger();

    SerialTask(EngineRuntime runtime, Runnable body) {
        this.runtime = runtime;
        this.body = body;
    }

    void request() {
        if (requests.getAndIncrement() == 0) {
            runtime.execute(this::drain);
        }
    }

    private void drain() {
        int served = 1;
        do {
            try {
                body.run();
            } catch (RuntimeException | Error e) {
                requests.set(0);
                throw e;
            }
            served = requests.addAndGet(-served);
        } while (served != 0);
    }
}
