package dev.tenaz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * Money transfers running on a cluster whose nodes are killed at random, over and over.
 *
 * <p>The bank deduplicates on the idempotency key, as a real payment API would. The claim under
 * test is the engine's whole contract: every workflow finishes, and every step takes effect
 * exactly once, even though the kills force many steps to execute more than once.
 */
class ChaosTest {

    private static final int ACCOUNTS = 10;
    private static final long OPENING_BALANCE = 1_000_000;
    private static final int TRANSFERS = 300;
    private static final int NODES = 3;
    private static final int KILLS = 60;

    public record Transfer(String from, String to, long amount) {}

    static final class Bank {
        private final Map<String, Long> balances = new HashMap<>();
        private final Set<String> applied = new HashSet<>();
        private int executions;

        synchronized void move(String idempotencyKey, String account, long delta) {
            executions++;
            if (applied.add(idempotencyKey)) {
                balances.merge(account, delta, Long::sum);
            }
        }

        synchronized long total() {
            return balances.values().stream().mapToLong(Long::longValue).sum();
        }

        synchronized long balance(String account) {
            return balances.get(account);
        }
    }

    private final Journal journal = new InMemoryJournal();
    private final Bank bank = new Bank();
    private int started;

    private final Workflow<Transfer, String> transfer = (ctx, t) -> {
        ctx.run("debit", step -> {
            jitter();
            bank.move(step.idempotencyKey(), t.from(), -t.amount());
            jitter();
        });
        ctx.sleep(Duration.ofMillis(5));
        ctx.run("credit", step -> {
            jitter();
            bank.move(step.idempotencyKey(), t.to(), t.amount());
            jitter();
        });
        return "ok";
    };

    private static void jitter() throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextInt(4));
    }

    private TenazEngine node() {
        return TenazEngine.builder(journal)
                .workerId("node-" + started++)
                .leaseTtl(Duration.ofMillis(200))
                .pollInterval(Duration.ofMillis(5))
                .build()
                .register("transfer", Transfer.class, String.class, transfer)
                .startWorkers();
    }

    @Test
    void everyTransferAppliesExactlyOnceWhileNodesKeepDying() throws Exception {
        Random random = new Random(42);
        for (int i = 0; i < ACCOUNTS; i++) {
            bank.balances.put("acc-" + i, OPENING_BALANCE);
        }
        Map<String, Long> expected = new HashMap<>(bank.balances);

        TenazEngine client = TenazEngine.builder(journal).build()
                .register("transfer", Transfer.class, String.class, transfer);
        List<TenazEngine> nodes = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            nodes.add(node());
        }

        // Transfers keep arriving while nodes die, so that kills land on work in every phase.
        List<WorkflowHandle<String>> handles = new ArrayList<>();
        for (int kill = 0; kill < KILLS; kill++) {
            for (int i = 0; i < TRANSFERS / KILLS; i++) {
                int from = random.nextInt(ACCOUNTS);
                int to = (from + 1 + random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
                Transfer t = new Transfer("acc-" + from, "acc-" + to, 1 + random.nextInt(500));
                expected.merge(t.from(), -t.amount(), Long::sum);
                expected.merge(t.to(), t.amount(), Long::sum);
                handles.add(client.start("transfer", "transfer-" + handles.size(), t));
            }
            Thread.sleep(random.nextInt(40));
            int victim = random.nextInt(NODES);
            nodes.get(victim).crash();
            nodes.set(victim, node());
        }

        for (WorkflowHandle<String> handle : handles) {
            assertEquals("ok", handle.result(Duration.ofSeconds(60)));
        }
        nodes.forEach(TenazEngine::close);

        int repeated = bank.executions - 2 * TRANSFERS;
        System.out.printf("chaos: %d transfers, %d node kills, %d step executions repeated after a kill%n",
                TRANSFERS, KILLS, repeated);
        assertEquals(ACCOUNTS * OPENING_BALANCE, bank.total(), "money is conserved");
        assertEquals(2 * TRANSFERS, bank.applied.size(), "every debit and credit took effect");
        for (String account : expected.keySet()) {
            assertEquals(expected.get(account), bank.balance(account), account);
        }
        assertTrue(repeated > 0, "the kills must have interrupted steps, or this test proves nothing");
    }
}
