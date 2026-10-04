package dev.tenaz.journal;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Journal that lives in the heap. It survives the crash of any engine that uses it, which makes
 * it the reference implementation for tests; it does not survive the JVM.
 */
public final class InMemoryJournal implements Journal {

    // ReentrantLock rather than synchronized: virtual threads wait here without pinning carriers.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<String, Entry> workflows = new HashMap<>();
    private final Set<String> active = new LinkedHashSet<>();
    private final List<Consumer<String>> listeners = new CopyOnWriteArrayList<>();

    private static final class Entry {
        final String type;
        final List<Event> events = new ArrayList<>();
        final Map<Integer, Instant> pendingTimers = new HashMap<>();
        WorkflowStatus status = WorkflowStatus.RUNNING;
        long processedVersion;
        long epoch;
        Instant leaseExpiry;

        Entry(String type) {
            this.type = type;
        }
    }

    @Override
    public boolean create(String workflowId, Event.WorkflowStarted started) {
        lock.lock();
        try {
            if (workflows.containsKey(workflowId)) {
                return false;
            }
            Entry entry = new Entry(started.workflowType());
            workflows.put(workflowId, entry);
            active.add(workflowId);
            append(workflowId, entry, started);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<History> load(String workflowId) {
        lock.lock();
        try {
            Entry entry = workflows.get(workflowId);
            if (entry == null) {
                return Optional.empty();
            }
            return Optional.of(new History(entry.type, List.copyOf(entry.events), entry.status));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now) {
        lock.lock();
        try {
            for (String id : active) {
                Entry entry = workflows.get(id);
                if (!workflowTypes.contains(entry.type)) {
                    continue;
                }
                boolean claimable = entry.leaseExpiry == null
                        ? entry.events.size() > entry.processedVersion
                        : !entry.leaseExpiry.isAfter(now);
                if (claimable) {
                    entry.epoch++;
                    entry.leaseExpiry = now.plus(ttl);
                    return Optional.of(new Lease(id, workerId, entry.epoch));
                }
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean renew(Lease lease, Duration ttl, Instant now) {
        lock.lock();
        try {
            Entry entry = workflows.get(lease.workflowId());
            if (!holds(entry, lease)) {
                return false;
            }
            entry.leaseExpiry = now.plus(ttl);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void appendDecisions(Lease lease, long expectedVersion, List<Event> events) {
        lock.lock();
        try {
            Entry entry = owned(lease);
            if (entry.events.size() != expectedVersion) {
                throw new VersionConflictException(lease.workflowId(), expectedVersion, entry.events.size());
            }
            for (Event event : events) {
                append(lease.workflowId(), entry, event);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void appendStepResult(Lease lease, Event event) {
        lock.lock();
        try {
            append(lease.workflowId(), owned(lease), event);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void appendExternal(String workflowId, Event event) {
        lock.lock();
        try {
            Entry entry = workflows.get(workflowId);
            if (entry == null) {
                throw new IllegalArgumentException("unknown workflow: " + workflowId);
            }
            if (entry.status == WorkflowStatus.RUNNING) {
                append(workflowId, entry, event);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int fireDueTimers(Instant now) {
        lock.lock();
        try {
            int fired = 0;
            for (String id : active) {
                Entry entry = workflows.get(id);
                Iterator<Map.Entry<Integer, Instant>> timers = entry.pendingTimers.entrySet().iterator();
                while (timers.hasNext()) {
                    Map.Entry<Integer, Instant> timer = timers.next();
                    if (!timer.getValue().isAfter(now)) {
                        timers.remove();
                        entry.events.add(new Event.TimerFired(timer.getKey()));
                        listeners.forEach(listener -> listener.accept(id));
                        fired++;
                    }
                }
            }
            if (fired > 0) {
                changed.signalAll();
            }
            return fired;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean park(Lease lease, long version) {
        lock.lock();
        try {
            Entry entry = owned(lease);
            if (entry.events.size() != version) {
                return false;
            }
            entry.processedVersion = version;
            entry.leaseExpiry = null;
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void abandon(Lease lease) {
        lock.lock();
        try {
            Entry entry = workflows.get(lease.workflowId());
            if (holds(entry, lease)) {
                entry.leaseExpiry = Instant.MIN;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Runnable subscribe(Consumer<String> listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    @Override
    public boolean awaitChange(String workflowId, long version, Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        lock.lock();
        try {
            Entry entry = workflows.get(workflowId);
            if (entry == null) {
                throw new IllegalArgumentException("unknown workflow: " + workflowId);
            }
            while (entry.events.size() <= version) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = changed.awaitNanos(nanos);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private Entry owned(Lease lease) {
        Entry entry = workflows.get(lease.workflowId());
        if (!holds(entry, lease)) {
            throw new FencedException(lease);
        }
        return entry;
    }

    private static boolean holds(Entry entry, Lease lease) {
        return entry != null
                && entry.status == WorkflowStatus.RUNNING
                && entry.leaseExpiry != null
                && entry.epoch == lease.epoch();
    }

    private void append(String workflowId, Entry entry, Event event) {
        entry.events.add(event);
        switch (event) {
            case Event.TimerStarted timer -> entry.pendingTimers.put(timer.seq(), timer.fireAt());
            case Event.WorkflowCompleted ignored -> finish(workflowId, entry, WorkflowStatus.COMPLETED);
            case Event.WorkflowFailed ignored -> finish(workflowId, entry, WorkflowStatus.FAILED);
            default -> { }
        }
        changed.signalAll();
        listeners.forEach(listener -> listener.accept(workflowId));
    }

    private void finish(String workflowId, Entry entry, WorkflowStatus status) {
        entry.status = status;
        entry.leaseExpiry = null;
        entry.pendingTimers.clear();
        active.remove(workflowId);
    }
}
