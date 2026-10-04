package dev.tenaz.bench;

import com.zaxxer.hikari.HikariDataSource;
import dev.tenaz.PostgresTestSupport;
import dev.tenaz.api.RetryPolicy;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import dev.tenaz.journal.PostgresJournal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Measures the engine on both journals. Run with:
 *
 * <pre>./mvnw -q test-compile exec:java -Dexec.mainClass=dev.tenaz.bench.Benchmark -Dexec.classpathScope=test</pre>
 *
 * Steps do nothing, so the numbers are the engine's own overhead: what it costs to make a step
 * durable, not what a step costs.
 */
public final class Benchmark {

    private static final Duration TIMEOUT = Duration.ofMinutes(10);
    private static final int STEPS = 5;
    private static final int CLIENTS = 16;

    private Benchmark() {}

    public static void main(String[] args) throws Exception {
        boolean quick = args.length > 0 && args[0].equals("quick");
        System.out.println("| journal | scenario | result |");
        System.out.println("|---|---|---|");

        Target memory = Target.memory();
        run(memory, quick ? 2_000 : 20_000, quick ? 500 : 5_000, quick ? 200 : 1_000);

        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")) {
            postgres.start();
            try (HikariDataSource dataSource = PostgresTestSupport.pool(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), 32)) {
                run(Target.postgres(dataSource), quick ? 500 : 3_000, quick ? 200 : 1_000, quick ? 200 : 1_000);
            }
        }
        System.exit(0);
    }

    private static void run(Target target, int burst, int longSteps, int samples) throws Exception {
        warmUp(target);
        burst(target, 1, burst);
        burst(target, 3, burst);
        latency(target, samples);
        longWorkflow(target, longSteps);
        if (target.journal instanceof AutoCloseable closeable) {
            closeable.close();
        }
    }

    /** @param dataSource null for the in-memory journal */
    private record Target(String name, Journal journal, HikariDataSource dataSource) {

        static Target memory() {
            return new Target("in-memory", new InMemoryJournal(), null);
        }

        static Target postgres(HikariDataSource dataSource) {
            PostgresJournal journal = new PostgresJournal(dataSource);
            journal.migrate();
            return new Target("PostgreSQL", journal, dataSource);
        }

        /** Waits for a run's workflows to finish without loading their histories from the database. */
        void awaitAll(String run, List<WorkflowHandle<Integer>> handles) throws Exception {
            if (dataSource == null) {
                for (WorkflowHandle<Integer> handle : handles) {
                    while (!handle.isDone()) {
                        Thread.sleep(1);
                    }
                }
                return;
            }
            String count = "SELECT count(*) FROM tenaz_workflows WHERE status = 'COMPLETED' AND id LIKE '" + run + "-%'";
            try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
                while (true) {
                    try (ResultSet rows = statement.executeQuery(count)) {
                        rows.next();
                        if (rows.getLong(1) == handles.size()) {
                            return;
                        }
                    }
                    Thread.sleep(2);
                }
            }
        }
    }

    private static final AtomicLong RUN = new AtomicLong();

    private static Workflow<Integer, Integer> steps() {
        return (ctx, count) -> {
            int sum = 0;
            for (int i = 0; i < count; i++) {
                int value = i;
                sum += ctx.step("step", Integer.class, RetryPolicy.none(), step -> value);
            }
            return sum;
        };
    }

    private static TenazEngine engine(Target target) {
        return TenazEngine.builder(target.journal)
                .maxConcurrentWorkflows(256)
                .build()
                .register("steps", Integer.class, Integer.class, steps());
    }

    private static List<TenazEngine> workers(Target target, int count) {
        List<TenazEngine> engines = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            engines.add(engine(target).startWorkers());
        }
        return engines;
    }

    private static void warmUp(Target target) throws Exception {
        List<TenazEngine> engines = workers(target, 1);
        String run = "warmup-" + RUN.incrementAndGet();
        List<WorkflowHandle<Integer>> handles = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            handles.add(engines.get(0).start("steps", run + "-" + i, STEPS));
        }
        for (WorkflowHandle<Integer> handle : handles) {
            handle.result(TIMEOUT);
        }
        engines.forEach(TenazEngine::close);
    }

    /** Throughput: start many workflows at once and time until the last one has finished. */
    private static void burst(Target target, int engineCount, int workflows) throws Exception {
        List<TenazEngine> engines = workers(target, engineCount);
        TenazEngine client = engine(target);
        String run = "burst-" + RUN.incrementAndGet();

        long started = System.nanoTime();
        // Started from several clients at once: one client starting workflows one after another
        // would measure that client, not the engines.
        List<WorkflowHandle<Integer>> handles = Collections.synchronizedList(new ArrayList<>());
        try (ExecutorService clients = Executors.newFixedThreadPool(CLIENTS)) {
            for (int c = 0; c < CLIENTS; c++) {
                int offset = c;
                clients.submit(() -> {
                    for (int i = offset; i < workflows; i += CLIENTS) {
                        handles.add(client.start("steps", run + "-" + i, STEPS));
                    }
                });
            }
        }
        target.awaitAll(run, handles);
        double seconds = (System.nanoTime() - started) / 1e9;
        engines.forEach(TenazEngine::close);

        report(target, engineCount + (engineCount == 1 ? " engine" : " engines") + ", " + workflows
                        + " workflows of " + STEPS + " steps",
                String.format(Locale.ROOT, "%,.0f workflows/s, %,.0f steps/s", workflows / seconds, workflows * STEPS / seconds));
    }

    /** Latency: one workflow at a time on an idle engine, from start() to the result arriving. */
    private static void latency(Target target, int samples) throws Exception {
        List<TenazEngine> engines = workers(target, 1);
        String run = "latency-" + RUN.incrementAndGet();
        long[] nanos = new long[samples];
        for (int i = 0; i < samples; i++) {
            long started = System.nanoTime();
            engines.get(0).<Integer>start("steps", run + "-" + i, STEPS).result(TIMEOUT);
            nanos[i] = System.nanoTime() - started;
        }
        engines.forEach(TenazEngine::close);
        Arrays.sort(nanos);
        report(target, "idle engine, one workflow of " + STEPS + " steps at a time",
                String.format(Locale.ROOT, "p50 %.1f ms, p99 %.1f ms", nanos[samples / 2] / 1e6, nanos[samples * 99 / 100] / 1e6));
    }

    /** How the cost of a step grows with the length of the history behind it. */
    private static void longWorkflow(Target target, int steps) throws Exception {
        List<TenazEngine> engines = workers(target, 1);
        long started = System.nanoTime();
        engines.get(0).<Integer>start("steps", "long-" + RUN.incrementAndGet(), steps).result(TIMEOUT);
        double seconds = (System.nanoTime() - started) / 1e9;
        engines.forEach(TenazEngine::close);
        report(target, "one workflow of " + steps + " sequential steps",
                String.format(Locale.ROOT, "%.1f s, %,.0f steps/s", seconds, steps / seconds));
    }

    private static void report(Target target, String scenario, String result) {
        System.out.println(String.format(Locale.ROOT, "| %s | %s | %s |", target.name, scenario, result));
    }
}
