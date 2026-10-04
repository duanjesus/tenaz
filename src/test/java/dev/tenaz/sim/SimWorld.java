package dev.tenaz.sim;

import dev.tenaz.engine.EngineRuntime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * A simulated world: one thread, one seeded source of randomness, and time that only moves when
 * the event loop moves it. Everything that happens is an event in a queue, so a run is a pure
 * function of its seed and can be replayed exactly.
 */
final class SimWorld {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final int MAX_TASK_JITTER_NANOS = 200_000;
    private static final int MAX_STEP_LATENCY_NANOS = 50_000_000;

    private record Scheduled(long at, long order, Node node, Runnable task) {}

    final Random random;
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>(
            Comparator.comparingLong(Scheduled::at).thenComparingLong(Scheduled::order));
    private final Node world = new Node("world", Duration.ZERO);
    private long now;
    private long order;
    private long executed;

    SimWorld(long seed) {
        this.random = new Random(seed);
    }

    Duration now() {
        return Duration.ofNanos(now);
    }

    long executed() {
        return executed;
    }

    Node node(String name, Duration clockSkew) {
        return new Node(name, clockSkew);
    }

    /** Schedules something that is not part of any node, such as a fault or a client request. */
    void at(Duration delay, Runnable task) {
        world.schedule(delay, task);
    }

    /** Runs events until the condition holds or simulated time reaches the limit. */
    boolean runUntil(BooleanSupplier condition, Duration limit) {
        while (!condition.getAsBoolean()) {
            Scheduled next = queue.poll();
            if (next == null || next.at > limit.toNanos()) {
                return false;
            }
            now = Math.max(now, next.at);
            Node node = next.node;
            if (!node.alive) {
                continue;
            }
            if (node.pausedUntil > now) {
                // A frozen process does its pending work, in order, when it thaws.
                queue.add(new Scheduled(node.pausedUntil, next.order, node, next.task));
                continue;
            }
            executed++;
            next.task.run();
        }
        return true;
    }

    /** One process. Its clock may be skewed, and it can be paused or killed at any event boundary. */
    final class Node implements EngineRuntime {
        final String name;
        private final long skewNanos;
        private boolean alive = true;
        private long pausedUntil;

        private Node(String name, Duration clockSkew) {
            this.name = name;
            this.skewNanos = clockSkew.toNanos();
        }

        boolean alive() {
            return alive;
        }

        void pause(Duration duration) {
            pausedUntil = Math.max(pausedUntil, now + duration.toNanos());
        }

        @Override
        public Clock clock() {
            return new Clock() {
                @Override
                public Instant instant() {
                    return START.plusNanos(now + skewNanos);
                }

                @Override
                public ZoneId getZone() {
                    return ZoneOffset.UTC;
                }

                @Override
                public Clock withZone(ZoneId zone) {
                    return this;
                }
            };
        }

        @Override
        public void execute(Runnable task) {
            enqueue(random.nextInt(MAX_TASK_JITTER_NANOS), task);
        }

        @Override
        public void schedule(Duration delay, Runnable task) {
            enqueue(delay.toNanos() + random.nextInt(MAX_TASK_JITTER_NANOS), task);
        }

        /**
         * The body takes effect at one instant and its outcome is reported at a later one, which
         * leaves a window for the node to die having done the work but not having recorded it.
         */
        @Override
        public <T> Runnable call(Callable<T> body, BiConsumer<T, Throwable> then) {
            boolean[] cancelled = {false};
            enqueue(random.nextInt(MAX_STEP_LATENCY_NANOS), () -> {
                if (cancelled[0]) {
                    return;
                }
                T result = null;
                Throwable error = null;
                try {
                    result = body.call();
                } catch (Throwable e) {
                    error = e;
                }
                T finalResult = result;
                Throwable finalError = error;
                enqueue(random.nextInt(MAX_STEP_LATENCY_NANOS), () -> {
                    if (!cancelled[0]) {
                        then.accept(finalResult, finalError);
                    }
                });
            });
            return () -> cancelled[0] = true;
        }

        @Override
        public void shutdown(boolean awaitTermination) {
            alive = false;
        }

        private void enqueue(long delayNanos, Runnable task) {
            queue.add(new Scheduled(now + delayNanos, order++, this, task));
        }
    }
}
