package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.tenaz.ChaosTest.Transfer;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.PostgresJournal;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * The chaos test with nothing simulated: the workers are separate JVMs, the journal and the bank
 * are in PostgreSQL, and the workers are destroyed by the operating system with no chance to
 * clean up.
 */
class KillNineTest {

    private static final int ACCOUNTS = 10;
    private static final long OPENING_BALANCE = 1_000_000;
    private static final int WORKERS = 3;
    private static final int KILLS = 12;
    private static final int TRANSFERS_PER_ROUND = 15;

    private final List<Process> workers = new ArrayList<>();
    private int spawned;

    @AfterEach
    void killWorkers() {
        workers.forEach(Process::destroyForcibly);
    }

    private Process spawn() throws IOException {
        PostgreSQLContainer<?> postgres = PostgresTestSupport.container();
        String java = ProcessHandle.current().info().command().orElse("java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        File log = new File("target", "killnine-worker-" + spawned++ + ".log");
        return new ProcessBuilder(java, "-cp", classpath, KillNineWorker.class.getName(),
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .redirectErrorStream(true)
                .redirectOutput(log)
                .start();
    }

    @Test
    void everyTransferAppliesExactlyOnceWhileWorkerProcessesAreKilled() throws Exception {
        Random random = new Random(7);
        PgBank bank = new PgBank(PostgresTestSupport.dataSource());
        bank.reset(ACCOUNTS, OPENING_BALANCE);
        Map<String, Long> expected = new HashMap<>();
        for (int i = 0; i < ACCOUNTS; i++) {
            expected.put("acc-" + i, OPENING_BALANCE);
        }

        try (PostgresJournal journal = PostgresTestSupport.freshJournal()) {
            TenazEngine client = TenazEngine.builder(journal).build()
                    .register("transfer", Transfer.class, String.class, bank.transferWorkflow());
            for (int i = 0; i < WORKERS; i++) {
                workers.add(spawn());
            }

            List<WorkflowHandle<String>> handles = new ArrayList<>();
            for (int kill = 0; kill < KILLS; kill++) {
                for (int i = 0; i < TRANSFERS_PER_ROUND; i++) {
                    int from = random.nextInt(ACCOUNTS);
                    int to = (from + 1 + random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
                    Transfer t = new Transfer("acc-" + from, "acc-" + to, 1 + random.nextInt(500));
                    expected.merge(t.from(), -t.amount(), Long::sum);
                    expected.merge(t.to(), t.amount(), Long::sum);
                    handles.add(client.start("transfer", "transfer-" + handles.size(), t));
                }
                Thread.sleep(600 + random.nextInt(400));
                // The oldest worker dies, so each one has had time to boot and pick up work.
                int victim = kill % WORKERS;
                workers.get(victim).destroyForcibly().waitFor();
                workers.set(victim, spawn());
            }

            for (WorkflowHandle<String> handle : handles) {
                assertEquals("ok", handle.result(Duration.ofSeconds(120)));
            }

            int transfers = handles.size();
            long repeated = bank.query("SELECT count(*) FROM bank_executions") - 2L * transfers;
            System.out.printf("kill -9: %d transfers, %d processes killed, %d step executions repeated%n",
                    transfers, KILLS, repeated);
            assertEquals(ACCOUNTS * OPENING_BALANCE, bank.query("SELECT sum(balance) FROM bank_accounts"),
                    "money is conserved");
            assertEquals(2L * transfers, bank.query("SELECT count(*) FROM bank_applied"),
                    "every debit and credit took effect");
            for (Map.Entry<String, Long> account : expected.entrySet()) {
                assertEquals(account.getValue(), bank.query(
                        "SELECT balance FROM bank_accounts WHERE id = '" + account.getKey() + "'"), account.getKey());
            }
        }
    }
}
