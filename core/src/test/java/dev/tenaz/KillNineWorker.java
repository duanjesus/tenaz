package dev.tenaz;

import dev.tenaz.ChaosTest.Transfer;
import dev.tenaz.engine.EngineObserver;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.PostgresJournal;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

/** A worker process for {@link KillNineTest} to destroy. Arguments: JDBC url, user, password. */
public final class KillNineWorker {

    private KillNineWorker() {}

    /** Writes to the worker's log what would explain a step running twice without a kill. */
    private static final class LeaseLog implements EngineObserver {
        private final Instant booted = Instant.now();
        private volatile Instant lastRenewal = booted;

        private String uptime() {
            return Duration.between(booted, Instant.now()).toMillis() + " ms after boot";
        }

        @Override
        public void leaseLost(String workflowId) {
            System.out.println("LEASE LOST " + workflowId + ", " + uptime());
        }

        @Override
        public void leasesRenewed(int count, Duration took) {
            Instant now = Instant.now();
            long gap = Duration.between(lastRenewal, now).toMillis();
            lastRenewal = now;
            if (gap > 600 || took.toMillis() > 300) {
                System.out.println("SLOW RENEWAL of " + count + " leases: took " + took.toMillis()
                        + " ms, " + gap + " ms since the one before, " + uptime());
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DataSource dataSource = PostgresTestSupport.pool(args[0], args[1], args[2], 8);
        TenazEngine.builder(new PostgresJournal(dataSource))
                .workerId("pid-" + ProcessHandle.current().pid())
                .leaseTtl(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(20))
                .observer(new LeaseLog())
                .build()
                .register("transfer", Transfer.class, String.class, new PgBank(dataSource).transferWorkflow())
                .startWorkers();
        Thread.sleep(Long.MAX_VALUE);
    }
}
