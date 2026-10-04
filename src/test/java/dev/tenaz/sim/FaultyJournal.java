package dev.tenaz.sim;

import dev.tenaz.journal.Event;
import dev.tenaz.journal.Journal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One node's connection to the journal, with the failures a real connection has: operations that
 * fail, operations that commit and then report failure, notifications that never arrive, and the
 * process dying immediately before or after any of them.
 */
final class FaultyJournal implements Journal {

    /** Injected in place of whatever a real driver would throw. */
    static final class SimulatedFailure extends RuntimeException {
        SimulatedFailure(String message) {
            super(message, null, false, false);
        }
    }

    /** What the simulation wants to go wrong, and where it counts what did. */
    static final class Faults {
        boolean enabled = true;
        boolean fencing = true;
        double failBefore = 0.02;
        double failAfter = 0.02;
        double crashBefore = 0.003;
        double crashAfter = 0.003;
        double dropNotification = 0.3;
        int failures;
        int crashes;
    }

    private final Journal delegate;
    private final SimWorld world;
    private final SimWorld.Node node;
    private final Faults faults;
    private Runnable crash = () -> { };

    FaultyJournal(Journal delegate, SimWorld world, SimWorld.Node node, Faults faults) {
        this.delegate = delegate;
        this.world = world;
        this.node = node;
        this.faults = faults;
    }

    void onCrash(Runnable crash) {
        this.crash = crash;
    }

    private <T> T attempt(Supplier<T> operation) {
        if (!faults.enabled) {
            return operation.get();
        }
        double roll = world.random.nextDouble();
        if ((roll -= faults.failBefore) < 0) {
            faults.failures++;
            throw new SimulatedFailure("failed before the operation");
        }
        if ((roll -= faults.crashBefore) < 0) {
            faults.crashes++;
            crash.run();
            throw new SimulatedFailure("crashed before the operation");
        }
        T result = operation.get();
        if ((roll -= faults.failAfter) < 0) {
            faults.failures++;
            throw new SimulatedFailure("failed after the operation committed");
        }
        if ((roll -= faults.crashAfter) < 0) {
            faults.crashes++;
            crash.run();
            throw new SimulatedFailure("crashed after the operation committed");
        }
        return result;
    }

    private void attempt(Runnable operation) {
        attempt(() -> {
            operation.run();
            return null;
        });
    }

    @Override
    public boolean create(String workflowId, Event.WorkflowStarted started) {
        return attempt(() -> delegate.create(workflowId, started));
    }

    @Override
    public Optional<History> load(String workflowId) {
        return attempt(() -> delegate.load(workflowId));
    }

    @Override
    public Optional<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now) {
        return attempt(() -> delegate.claim(workerId, workflowTypes, ttl, now));
    }

    @Override
    public boolean renew(Lease lease, Duration ttl, Instant now) {
        return attempt(() -> delegate.renew(lease, ttl, now));
    }

    @Override
    public void appendDecisions(Lease lease, long expectedVersion, List<Event> events) {
        attempt(() -> delegate.appendDecisions(lease, expectedVersion, events));
    }

    @Override
    public void appendStepResult(Lease lease, Event event) {
        attempt(() -> {
            try {
                delegate.appendStepResult(lease, event);
            } catch (FencedException e) {
                if (faults.fencing) {
                    throw e;
                }
                // The deliberately broken journal: a worker that lost its lease still gets to write.
                delegate.appendExternal(lease.workflowId(), event);
            }
        });
    }

    @Override
    public void appendExternal(String workflowId, Event event) {
        attempt(() -> delegate.appendExternal(workflowId, event));
    }

    @Override
    public int fireDueTimers(Instant now) {
        return attempt(() -> delegate.fireDueTimers(now));
    }

    @Override
    public boolean park(Lease lease, long version) {
        return attempt(() -> delegate.park(lease, version));
    }

    @Override
    public void abandon(Lease lease) {
        attempt(() -> delegate.abandon(lease));
    }

    @Override
    public Runnable subscribe(Consumer<String> listener) {
        return delegate.subscribe(workflowId -> {
            if (world.random.nextDouble() >= faults.dropNotification) {
                node.execute(() -> listener.accept(workflowId));
            }
        });
    }

    @Override
    public boolean awaitChange(String workflowId, long version, Duration timeout) {
        throw new UnsupportedOperationException("nothing blocks in a simulation");
    }
}
