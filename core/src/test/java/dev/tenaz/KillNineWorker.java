package dev.tenaz;

import dev.tenaz.ChaosTest.Transfer;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.PostgresJournal;
import java.time.Duration;
import javax.sql.DataSource;

/** A worker process for {@link KillNineTest} to destroy. Arguments: JDBC url, user, password. */
public final class KillNineWorker {

    private KillNineWorker() {}

    public static void main(String[] args) throws InterruptedException {
        DataSource dataSource = PostgresTestSupport.pool(args[0], args[1], args[2], 8);
        TenazEngine.builder(new PostgresJournal(dataSource))
                .workerId("pid-" + ProcessHandle.current().pid())
                .leaseTtl(Duration.ofSeconds(1))
                .pollInterval(Duration.ofMillis(20))
                .build()
                .register("transfer", Transfer.class, String.class, new PgBank(dataSource).transferWorkflow())
                .startWorkers();
        Thread.sleep(Long.MAX_VALUE);
    }
}
