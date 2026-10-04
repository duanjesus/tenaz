package dev.tenaz.sim;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.Workflow;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.Event;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal.History;
import dev.tenaz.journal.Journal.WorkflowStatus;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One simulated run of a three-node cluster under one seed.
 *
 * <p>For thirty simulated seconds clients start workflows while the world misbehaves: nodes
 * crash and restart, freeze for longer than their leases, run on skewed clocks, lose
 * notifications, and see journal operations fail before or after committing. Then the faults
 * stop, and the run must converge: every workflow finished, every effect applied exactly once.
 */
final class Simulation {

    public record Transfer(String from, String to, long amount) {}

    record Report(int fingerprint, long events, Duration simulatedTime, int crashes, int pauses,
                  int journalFailures, int repeatedExecutions) {}

    private static final Duration LEASE_TTL = Duration.ofSeconds(2);
    private static final Duration POLL = Duration.ofMillis(200);
    private static final Duration CHAOS = Duration.ofSeconds(30);
    private static final Duration LIMIT = CHAOS.plusMinutes(5);
    private static final Duration HOLD_DEADLINE = Duration.ofSeconds(3);
    private static final long MAX_SKEW_MILLIS = 500;
    private static final int NODES = 3;
    private static final int ACCOUNTS = 5;
    private static final int TRANSFERS = 20;
    private static final int HOLDS = 10;
    private static final long OPENING_BALANCE = 10_000;

    private final long seed;
    private final SimWorld world;
    private final InMemoryJournal journal = new InMemoryJournal();
    private final FaultyJournal.Faults faults = new FaultyJournal.Faults();
    private final TenazEngine[] engines = new TenazEngine[NODES];
    private final SimWorld.Node[] nodes = new SimWorld.Node[NODES];
    private final TenazEngine client;

    // The outside world that steps act on. It deduplicates on the idempotency key.
    private final Map<String, String> applied = new LinkedHashMap<>();
    private final Map<String, Long> balances = new TreeMap<>();
    private final Map<String, Long> expectedBalances = new TreeMap<>();
    private final Set<String> promptlyApproved = new HashSet<>();
    private final Set<String> neverApproved = new HashSet<>();
    private final List<String> workflowIds = new ArrayList<>();
    private int executions;
    private int incarnations;
    private int crashes;
    private int pauses;

    private Simulation(long seed, boolean fencing) {
        this.seed = seed;
        this.world = new SimWorld(seed);
        this.faults.fencing = fencing;
        this.client = register(TenazEngine.builder(journal)
                .runtime(world.node("client", Duration.ZERO))
                .build());
    }

    static Report run(long seed) {
        return new Simulation(seed, true).run();
    }

    /** The same run against a journal that lets workers write after losing their lease. */
    static Report runWithoutFencing(long seed) {
        return new Simulation(seed, false).run();
    }

    private Report run() {
        for (int i = 0; i < ACCOUNTS; i++) {
            balances.put("acc-" + i, OPENING_BALANCE);
        }
        expectedBalances.putAll(balances);
        for (int i = 0; i < NODES; i++) {
            boot(i);
        }
        scheduleWorkload();
        scheduleFaults();
        world.at(CHAOS, this::heal);

        boolean converged = world.runUntil(this::allFinished, LIMIT);
        check(converged, "not every workflow finished within " + LIMIT + " of simulated time: " + unfinished());
        verify();
        return new Report(fingerprint(), world.executed(), world.now(), crashes + faults.crashes, pauses,
                faults.failures, executions - applied.size());
    }

    private TenazEngine register(TenazEngine engine) {
        return engine
                .register("transfer", Transfer.class, String.class, transfer())
                .register("hold", String.class, String.class, hold());
    }

    private Workflow<Transfer, String> transfer() {
        return (ctx, t) -> {
            ctx.run("debit", step -> move(step.idempotencyKey(), t.from(), -t.amount()));
            ctx.sleep(Duration.ofMillis(50));
            ctx.run("credit", step -> move(step.idempotencyKey(), t.to(), t.amount()));
            return "ok";
        };
    }

    /** Captures a reservation if it is approved before a deadline, and releases it otherwise. */
    private Workflow<String, String> hold() {
        return (ctx, input) -> {
            DurablePromise<String> approval = ctx.signal("approve", String.class);
            DurablePromise<Void> deadline = ctx.timer(HOLD_DEADLINE);
            ctx.run("reserve", step -> effect(step.idempotencyKey(), "reserve"));
            if (ctx.anyOf(approval, deadline) == approval) {
                ctx.run("capture", step -> effect(step.idempotencyKey(), "capture"));
                return "captured";
            }
            ctx.run("release", step -> effect(step.idempotencyKey(), "release"));
            return "released";
        };
    }

    private boolean effect(String idempotencyKey, String label) {
        executions++;
        return applied.putIfAbsent(idempotencyKey, label) == null;
    }

    private void move(String idempotencyKey, String account, long delta) {
        if (effect(idempotencyKey, "move")) {
            balances.merge(account, delta, Long::sum);
        }
    }

    private void boot(int slot) {
        long skew = world.random.nextLong(-MAX_SKEW_MILLIS, MAX_SKEW_MILLIS + 1);
        SimWorld.Node node = world.node("node-" + slot + "." + incarnations++, Duration.ofMillis(skew));
        FaultyJournal connection = new FaultyJournal(journal, world, node, faults);
        TenazEngine engine = register(TenazEngine.builder(connection)
                .runtime(node)
                .workerId(node.name)
                .leaseTtl(LEASE_TTL)
                .pollInterval(POLL)
                .build());
        connection.onCrash(engine::crash);
        nodes[slot] = node;
        engines[slot] = engine;
        engine.startWorkers();
    }

    private void scheduleWorkload() {
        for (int i = 0; i < TRANSFERS; i++) {
            String id = "transfer-" + i;
            int from = world.random.nextInt(ACCOUNTS);
            int to = (from + 1 + world.random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
            Transfer t = new Transfer("acc-" + from, "acc-" + to, 1 + world.random.nextInt(500));
            expectedBalances.merge(t.from(), -t.amount(), Long::sum);
            expectedBalances.merge(t.to(), t.amount(), Long::sum);
            workflowIds.add(id);
            world.at(randomDuration(CHAOS), () -> client.start("transfer", id, t));
        }
        for (int i = 0; i < HOLDS; i++) {
            String id = "hold-" + i;
            Duration start = randomDuration(CHAOS);
            workflowIds.add(id);
            if (world.random.nextInt(3) == 0) {
                neverApproved.add(id);
                world.at(start, () -> client.start("hold", id, id));
                continue;
            }
            // Approvals cluster around the deadline, where the race with the timer is closest.
            Duration delay = randomDuration(HOLD_DEADLINE.multipliedBy(2));
            if (delay.compareTo(Duration.ofSeconds(1)) < 0) {
                promptlyApproved.add(id);
            }
            // Scheduled by the start itself: a workflow cannot be signalled before it exists.
            world.at(start, () -> {
                client.start("hold", id, id);
                world.at(delay, () -> client.handle(id, String.class).signal("approve", "ok"));
            });
        }
    }

    private void scheduleFaults() {
        Duration at = Duration.ZERO;
        while (true) {
            at = at.plus(randomDuration(Duration.ofSeconds(3)));
            if (at.compareTo(CHAOS) >= 0) {
                return;
            }
            int slot = world.random.nextInt(NODES);
            if (world.random.nextBoolean()) {
                Duration downtime = randomDuration(Duration.ofSeconds(4));
                world.at(at, () -> {
                    if (nodes[slot].alive()) {
                        crashes++;
                        engines[slot].crash();
                    }
                    world.at(downtime, () -> reboot(slot));
                });
            } else {
                // Long enough, often, for the node to come back as a zombie holding expired leases.
                Duration freeze = randomDuration(LEASE_TTL.multipliedBy(3));
                world.at(at, () -> {
                    pauses++;
                    nodes[slot].pause(freeze);
                });
            }
        }
    }

    private void reboot(int slot) {
        if (!nodes[slot].alive()) {
            boot(slot);
        }
    }

    private void heal() {
        faults.enabled = false;
        for (int slot = 0; slot < NODES; slot++) {
            reboot(slot);
        }
    }

    private Duration randomDuration(Duration max) {
        return Duration.ofNanos(world.random.nextLong(max.toNanos()));
    }

    private boolean allFinished() {
        return !faults.enabled && unfinished().isEmpty();
    }

    private List<String> unfinished() {
        List<String> running = new ArrayList<>();
        for (String id : workflowIds) {
            if (journal.load(id).map(history -> history.status() == WorkflowStatus.RUNNING).orElse(true)) {
                running.add(id);
            }
        }
        return running;
    }

    private void verify() {
        for (String id : workflowIds) {
            History history = journal.load(id).orElseThrow();
            checkWellFormed(id, history.events());
            Event last = history.events().getLast();
            check(last instanceof Event.WorkflowCompleted, id + " did not complete: " + last);
            String result = ((Event.WorkflowCompleted) last).result();
            Set<String> effects = effectsOf(id);
            if (id.startsWith("hold-")) {
                String branch = result.equals("\"captured\"") ? "capture" : "release";
                check(effects.equals(Set.of("reserve", branch)),
                        id + " returned " + result + " but its effects were " + effects);
                check(!neverApproved.contains(id) || branch.equals("release"), id + " captured without approval");
                check(!promptlyApproved.contains(id) || branch.equals("capture"),
                        id + " released although it was approved well before the deadline");
            }
        }
        check(applied.size() == 2 * TRANSFERS + 2 * HOLDS, "expected every step to take effect once, but "
                + applied.size() + " effects were applied");
        check(balances.equals(expectedBalances), "balances " + balances + " but expected " + expectedBalances);
    }

    /** Invariants every history must satisfy whatever the workflow does. */
    private void checkWellFormed(String id, List<Event> events) {
        Set<Integer> commands = new HashSet<>();
        Set<Integer> resolved = new HashSet<>();
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            switch (event) {
                case Event.WorkflowStarted e -> check(i == 0, id + ": WorkflowStarted at position " + i);
                case Event.StepScheduled e -> check(commands.add(e.seq()), id + ": command issued twice: " + e);
                case Event.TimerStarted e -> check(commands.add(e.seq()), id + ": command issued twice: " + e);
                case Event.SideEffectRecorded e -> check(commands.add(e.seq()), id + ": command issued twice: " + e);
                case Event.StepCompleted e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.StepFailed e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.TimerFired e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.SignalReceived e -> { }
                case Event.WorkflowCompleted e -> check(i == events.size() - 1, id + ": events after completion");
                case Event.WorkflowFailed e -> check(i == events.size() - 1, id + ": events after failure");
            }
        }
        // A step may only take effect after the intent to run it is durable.
        for (String key : applied.keySet()) {
            if (key.startsWith(id + "/")) {
                int seq = Integer.parseInt(key.substring(id.length() + 1));
                check(commands.contains(seq), id + ": step #" + seq + " took effect without being scheduled");
            }
        }
    }

    private void checkResolution(String id, Event event, int seq, Set<Integer> commands, Set<Integer> resolved) {
        check(commands.contains(seq), id + ": outcome before its command: " + event);
        check(resolved.add(seq), id + ": command resolved twice: " + event);
    }

    private Set<String> effectsOf(String id) {
        Set<String> effects = new TreeSet<>();
        applied.forEach((key, label) -> {
            if (key.startsWith(id + "/")) {
                effects.add(label);
            }
        });
        return effects;
    }

    private int fingerprint() {
        int hash = Long.hashCode(world.executed());
        for (String id : workflowIds) {
            hash = 31 * hash + journal.load(id).orElseThrow().events().toString().hashCode();
        }
        return hash;
    }

    private void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("seed " + seed + ": " + message
                    + " (reproduce with -Dtenaz.sim.seed=" + seed + ")");
        }
    }
}
