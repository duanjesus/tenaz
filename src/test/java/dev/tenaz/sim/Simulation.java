package dev.tenaz.sim;

import dev.tenaz.api.ChildWorkflowFailedException;
import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowCancelledException;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * One simulated run of a three-node cluster under one seed.
 *
 * <p>For thirty simulated seconds clients start, signal and cancel workflows while the world
 * misbehaves: nodes crash and restart, freeze for longer than their leases, run on skewed
 * clocks, lose notifications, and see journal operations fail before or after committing. Then
 * the faults stop, and the run must converge: every workflow ended, every effect applied exactly
 * once, and no money created or lost whatever was cancelled halfway.
 */
final class Simulation {

    public record Transfer(String from, String to, long amount) {}

    public record Batch(List<Transfer> transfers) {}

    record Report(int fingerprint, long events, Duration simulatedTime, int crashes, int pauses,
                  int journalFailures, int repeatedExecutions, int cancelled) {}

    private static final Duration LEASE_TTL = Duration.ofSeconds(2);
    private static final Duration POLL = Duration.ofMillis(200);
    private static final Duration CHAOS = Duration.ofSeconds(30);
    private static final Duration LIMIT = CHAOS.plusMinutes(5);
    private static final Duration HOLD_DEADLINE = Duration.ofSeconds(3);
    private static final long MAX_SKEW_MILLIS = 500;
    private static final int NODES = 3;
    private static final int ACCOUNTS = 5;
    private static final int TRANSFERS = 14;
    private static final int BATCHES = 4;
    private static final int BATCH_SIZE = 2;
    private static final int HOLDS = 8;
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
    // Workflows a client starts, and the children some of them start in turn.
    private final List<String> roots = new ArrayList<>();
    private final Map<String, Transfer> transfers = new LinkedHashMap<>();
    private final Map<String, List<String>> batchChildren = new LinkedHashMap<>();
    private final Set<String> promptlyApproved = new HashSet<>();
    private final Set<String> neverApproved = new HashSet<>();
    private final Set<String> cancelRequested = new HashSet<>();
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
        for (int i = 0; i < NODES; i++) {
            boot(i);
        }
        scheduleWorkload();
        scheduleFaults();
        world.at(CHAOS, this::heal);

        boolean converged = world.runUntil(this::allEnded, LIMIT);
        check(converged, "not every workflow ended within " + LIMIT + " of simulated time: " + unfinished());
        int cancelled = verify();
        return new Report(fingerprint(), world.executed(), world.now(), crashes + faults.crashes, pauses,
                faults.failures, executions - applied.size(), cancelled);
    }

    private TenazEngine register(TenazEngine engine) {
        return engine
                .register("transfer", Transfer.class, String.class, transfer())
                .register("batch", Batch.class, String.class, batch())
                .register("hold", String.class, String.class, hold());
    }

    /**
     * Moves money, and puts it back if cancelled between the debit and the credit. A step that
     * was already running when the cancellation arrived is waited for, because whether it took
     * effect decides what there is to undo.
     */
    private Workflow<Transfer, String> transfer() {
        return (ctx, t) -> {
            DurablePromise<Void> debit = null;
            DurablePromise<Void> credit = null;
            try {
                debit = ctx.stepAsync("debit", Void.class,
                        step -> move(step.idempotencyKey(), "debit", t.from(), -t.amount()));
                debit.get();
                ctx.sleep(Duration.ofMillis(50));
                credit = ctx.stepAsync("credit", Void.class,
                        step -> move(step.idempotencyKey(), "credit", t.to(), t.amount()));
                credit.get();
                return "ok";
            } catch (WorkflowCancelledException e) {
                if (debit == null) {
                    throw e;
                }
                debit.get();
                if (credit != null) {
                    credit.get();
                    return "ok";
                }
                ctx.run("refund", step -> move(step.idempotencyKey(), "refund", t.from(), t.amount()));
                throw e;
            }
        };
    }

    /** Runs transfers as child workflows and reports how many went through. */
    private Workflow<Batch, String> batch() {
        return (ctx, batch) -> {
            List<DurablePromise<String>> children = new ArrayList<>();
            for (int i = 0; i < batch.transfers().size(); i++) {
                children.add(ctx.childAsync("transfer", ctx.workflowId() + "/" + i,
                        batch.transfers().get(i), String.class));
            }
            int completed = 0;
            for (DurablePromise<String> child : children) {
                try {
                    child.get();
                    completed++;
                } catch (ChildWorkflowFailedException e) {
                    // cancelled along the way; the batch carries on with the rest
                }
            }
            return "completed " + completed;
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

    private Void move(String idempotencyKey, String label, String account, long delta) {
        if (effect(idempotencyKey, label)) {
            balances.merge(account, delta, Long::sum);
        }
        return null;
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

    private Transfer randomTransfer() {
        int from = world.random.nextInt(ACCOUNTS);
        int to = (from + 1 + world.random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
        return new Transfer("acc-" + from, "acc-" + to, 1 + world.random.nextInt(500));
    }

    private void scheduleWorkload() {
        for (int i = 0; i < TRANSFERS; i++) {
            String id = "transfer-" + i;
            Transfer t = randomTransfer();
            transfers.put(id, t);
            startThen(id, "transfer", t, maybeCancel(id, 4));
        }
        for (int i = 0; i < BATCHES; i++) {
            String id = "batch-" + i;
            List<Transfer> items = new ArrayList<>();
            List<String> children = new ArrayList<>();
            for (int c = 0; c < BATCH_SIZE; c++) {
                Transfer t = randomTransfer();
                items.add(t);
                children.add(id + "/" + c);
                transfers.put(id + "/" + c, t);
            }
            batchChildren.put(id, children);
            startThen(id, "batch", new Batch(items), maybeCancel(id, 2));
        }
        for (int i = 0; i < HOLDS; i++) {
            String id = "hold-" + i;
            Runnable cancel = maybeCancel(id, 5);
            if (world.random.nextInt(3) == 0) {
                neverApproved.add(id);
                startThen(id, "hold", id, cancel);
                continue;
            }
            // Approvals cluster around the deadline, where the race with the timer is closest.
            Duration delay = randomDuration(HOLD_DEADLINE.multipliedBy(2));
            if (delay.compareTo(Duration.ofSeconds(1)) < 0) {
                promptlyApproved.add(id);
            }
            startThen(id, "hold", id, () -> {
                world.at(delay, () -> client.handle(id, String.class).signal("approve", "ok"));
                cancel.run();
            });
        }
    }

    /** Whatever a client does to a workflow is scheduled by its start: it has to exist first. */
    private void startThen(String id, String type, Object input, Runnable afterwards) {
        roots.add(id);
        world.at(randomDuration(CHAOS), () -> {
            client.start(type, id, input);
            afterwards.run();
        });
    }

    private Runnable maybeCancel(String id, int oneIn) {
        if (world.random.nextInt(oneIn) != 0) {
            return () -> { };
        }
        cancelRequested.add(id);
        Duration delay = randomDuration(Duration.ofSeconds(4));
        return () -> world.at(delay, () -> client.handle(id, String.class).cancel("changed my mind"));
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

    private boolean allEnded() {
        return !faults.enabled && unfinished().isEmpty();
    }

    private List<String> unfinished() {
        List<String> running = new ArrayList<>();
        for (String id : roots) {
            if (statusOf(id).map(status -> status == WorkflowStatus.RUNNING).orElse(true)) {
                running.add(id);
            }
            // A child that was never created is no concern once its parent has ended.
            for (String child : batchChildren.getOrDefault(id, List.of())) {
                if (statusOf(child).orElse(null) == WorkflowStatus.RUNNING) {
                    running.add(child);
                }
            }
        }
        return running;
    }

    private Optional<WorkflowStatus> statusOf(String id) {
        return journal.load(id).map(History::status);
    }

    /** Returns how many workflows ended cancelled. */
    private int verify() {
        int cancelled = 0;
        Map<String, Long> expectedBalances = new TreeMap<>();
        balances.keySet().forEach(account -> expectedBalances.put(account, OPENING_BALANCE));

        for (Map.Entry<String, Transfer> entry : transfers.entrySet()) {
            String id = entry.getKey();
            Transfer t = entry.getValue();
            Optional<History> history = journal.load(id);
            if (history.isEmpty()) {
                check(effectsOf(id).isEmpty(), id + " never existed but has effects " + effectsOf(id));
                continue;
            }
            checkWellFormed(id, history.get().events());
            Set<String> effects = effectsOf(id);
            switch (history.get().status()) {
                case COMPLETED -> {
                    check(effects.equals(Set.of("credit", "debit")), id + " completed with effects " + effects);
                    expectedBalances.merge(t.from(), -t.amount(), Long::sum);
                    expectedBalances.merge(t.to(), t.amount(), Long::sum);
                }
                case CANCELLED -> {
                    cancelled++;
                    check(effects.isEmpty() || effects.equals(Set.of("debit", "refund")),
                            id + " was cancelled with effects " + effects);
                }
                default -> check(false, id + " ended as " + history.get().events().getLast());
            }
        }
        check(balances.equals(expectedBalances), "balances " + balances + " but expected " + expectedBalances);

        for (Map.Entry<String, List<String>> entry : batchChildren.entrySet()) {
            String id = entry.getKey();
            History history = journal.load(id).orElseThrow();
            checkWellFormed(id, history.events());
            if (history.status() == WorkflowStatus.CANCELLED) {
                cancelled++;
                check(cancelRequested.contains(id), id + " was cancelled though nobody asked");
                continue;
            }
            // A batch that ran to the end must report exactly what happened to its children.
            long completed = entry.getValue().stream()
                    .filter(child -> statusOf(child).orElse(null) == WorkflowStatus.COMPLETED).count();
            Event last = history.events().getLast();
            check(last.equals(new Event.WorkflowCompleted("\"completed " + completed + "\"")),
                    id + " ended as " + last + " but " + completed + " of its children completed");
        }

        for (String id : roots) {
            if (!id.startsWith("hold-")) {
                continue;
            }
            History history = journal.load(id).orElseThrow();
            checkWellFormed(id, history.events());
            Set<String> effects = effectsOf(id);
            check(!(effects.contains("capture") && effects.contains("release")),
                    id + " both captured and released: " + effects);
            if (history.status() == WorkflowStatus.CANCELLED) {
                cancelled++;
                check(cancelRequested.contains(id), id + " was cancelled though nobody asked");
                continue;
            }
            Event last = history.events().getLast();
            check(last instanceof Event.WorkflowCompleted, id + " did not complete: " + last);
            String branch = ((Event.WorkflowCompleted) last).result().equals("\"captured\"") ? "capture" : "release";
            check(effects.equals(Set.of("reserve", branch)), id + " ended as " + last + " with effects " + effects);
            check(!neverApproved.contains(id) || branch.equals("release"), id + " captured without approval");
            check(!promptlyApproved.contains(id) || branch.equals("capture"),
                    id + " released although it was approved well before the deadline");
        }
        return cancelled;
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
                case Event.ChildStarted e -> check(commands.add(e.seq()), id + ": command issued twice: " + e);
                case Event.StepCompleted e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.StepFailed e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.TimerFired e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.ChildCompleted e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.ChildFailed e -> checkResolution(id, e, e.seq(), commands, resolved);
                case Event.SignalReceived e -> { }
                case Event.CancelRequested e -> { }
                case Event.VersionMarked e -> { }
                case Event.WorkflowCompleted e -> check(i == events.size() - 1, id + ": events after completion");
                case Event.WorkflowFailed e -> check(i == events.size() - 1, id + ": events after failure");
                case Event.WorkflowCancelled e -> check(i == events.size() - 1, id + ": events after cancellation");
            }
        }
        // A step may only take effect after the intent to run it is durable.
        for (String key : applied.keySet()) {
            if (key.startsWith(id + "/") && key.indexOf('/', id.length() + 1) < 0) {
                int seq = Integer.parseInt(key.substring(id.length() + 1));
                check(commands.contains(seq), id + ": step #" + seq + " took effect without being scheduled");
            }
        }
    }

    private void checkResolution(String id, Event event, int seq, Set<Integer> commands, Set<Integer> resolved) {
        check(commands.contains(seq), id + ": outcome before its command: " + event);
        check(resolved.add(seq), id + ": command resolved twice: " + event);
    }

    /** The effects of the workflow's own steps: its idempotency keys are its id, a slash and a number. */
    private Set<String> effectsOf(String id) {
        Set<String> effects = new TreeSet<>();
        applied.forEach((key, label) -> {
            if (key.startsWith(id + "/") && key.indexOf('/', id.length() + 1) < 0) {
                effects.add(label);
            }
        });
        return effects;
    }

    private int fingerprint() {
        int hash = Long.hashCode(world.executed());
        List<String> ids = new ArrayList<>(roots);
        batchChildren.values().forEach(ids::addAll);
        for (String id : ids) {
            hash = 31 * hash + journal.load(id).map(history -> history.events().toString().hashCode()).orElse(0);
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
