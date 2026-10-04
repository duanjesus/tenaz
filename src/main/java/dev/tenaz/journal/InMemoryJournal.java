package dev.tenaz.journal;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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
    // Workflows a claim could pick: leased ones, and unleased ones with unprocessed events.
    private final Set<String> candidates = new LinkedHashSet<>();
    private final Set<String> withTimers = new LinkedHashSet<>();
    private final List<ChangeListener> listeners = new CopyOnWriteArrayList<>();

    private static final class Entry {
        final String type;
        final List<Event> events = new ArrayList<>();
        final Map<Integer, Instant> pendingTimers = new HashMap<>();
        WorkflowStatus status = WorkflowStatus.RUNNING;
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
    public List<Event> loadSince(String workflowId, long fromVersion) {
        lock.lock();
        try {
            Entry entry = workflows.get(workflowId);
            if (entry == null || fromVersion >= entry.events.size()) {
                return List.of();
            }
            return List.copyOf(entry.events.subList((int) fromVersion, entry.events.size()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now, int limit) {
        lock.lock();
        try {
            List<Lease> claimed = new ArrayList<>();
            for (String id : candidates) {
                if (claimed.size() >= limit) {
                    break;
                }
                Entry entry = workflows.get(id);
                boolean claimable = entry.leaseExpiry == null || !entry.leaseExpiry.isAfter(now);
                if (claimable && workflowTypes.contains(entry.type)) {
                    entry.epoch++;
                    entry.leaseExpiry = now.plus(ttl);
                    claimed.add(new Lease(id, workerId, entry.epoch));
                }
            }
            return claimed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<Lease> renew(Collection<Lease> leases, Duration ttl, Instant now) {
        lock.lock();
        try {
            Set<Lease> held = new LinkedHashSet<>();
            for (Lease lease : leases) {
                Entry entry = workflows.get(lease.workflowId());
                if (holds(entry, lease)) {
                    entry.leaseExpiry = now.plus(ttl);
                    held.add(lease);
                }
            }
            return held;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void append(Lease lease, long expectedVersion, List<Event> events) {
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
            for (String id : List.copyOf(withTimers)) {
                Entry entry = workflows.get(id);
                List<Integer> due = new ArrayList<>();
                Iterator<Map.Entry<Integer, Instant>> timers = entry.pendingTimers.entrySet().iterator();
                while (timers.hasNext()) {
                    Map.Entry<Integer, Instant> timer = timers.next();
                    if (!timer.getValue().isAfter(now)) {
                        timers.remove();
                        due.add(timer.getKey());
                    }
                }
                for (int seq : due) {
                    append(id, entry, new Event.TimerFired(seq));
                    fired++;
                }
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
            entry.leaseExpiry = null;
            candidates.remove(lease.workflowId());
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
    public Runnable subscribe(ChangeListener listener) {
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
        // An event on a workflow nobody owns is work for someone to claim.
        candidates.add(workflowId);
        switch (event) {
            case Event.TimerStarted timer -> {
                entry.pendingTimers.put(timer.seq(), timer.fireAt());
                withTimers.add(workflowId);
            }
            case Event.WorkflowCompleted ignored -> finish(workflowId, entry, WorkflowStatus.COMPLETED);
            case Event.WorkflowFailed ignored -> finish(workflowId, entry, WorkflowStatus.FAILED);
            default -> { }
        }
        if (entry.pendingTimers.isEmpty()) {
            withTimers.remove(workflowId);
        }
        changed.signalAll();
        long version = entry.events.size();
        boolean claimable = entry.status == WorkflowStatus.RUNNING && entry.leaseExpiry == null;
        listeners.forEach(listener -> listener.changed(workflowId, version, claimable));
    }

    private void finish(String workflowId, Entry entry, WorkflowStatus status) {
        entry.status = status;
        entry.leaseExpiry = null;
        entry.pendingTimers.clear();
        candidates.remove(workflowId);
    }
}
