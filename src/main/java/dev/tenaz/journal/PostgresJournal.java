package dev.tenaz.journal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * Journal backed by PostgreSQL.
 *
 * <p>Every operation is one transaction that takes the workflow's row lock first, so operations
 * on the same workflow are serialized and the lease epoch and history version are checked against
 * committed state. Claims use {@code FOR UPDATE SKIP LOCKED}, so workers never queue behind each
 * other. Waiters are woken through {@code LISTEN/NOTIFY}, with polling as a fallback in case a
 * notification is lost with its connection.
 */
public final class PostgresJournal implements Journal, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(PostgresJournal.class.getName());
    private static final String CHANNEL = "tenaz_changes";
    private static final long MIGRATION_LOCK = 7_361_001L;
    private static final long FALLBACK_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final int TIMER_BATCH = 200;

    private static final String SCHEMA = """
            CREATE TABLE IF NOT EXISTS tenaz_workflows (
                id                text PRIMARY KEY,
                type              text NOT NULL,
                status            text NOT NULL DEFAULT 'RUNNING',
                version           bigint NOT NULL DEFAULT 0,
                processed_version bigint NOT NULL DEFAULT 0,
                epoch             bigint NOT NULL DEFAULT 0,
                lease_owner       text,
                lease_expiry      timestamptz
            );
            CREATE INDEX IF NOT EXISTS tenaz_workflows_running
                ON tenaz_workflows (type) WHERE status = 'RUNNING';
            CREATE TABLE IF NOT EXISTS tenaz_events (
                workflow_id text NOT NULL REFERENCES tenaz_workflows (id) ON DELETE CASCADE,
                seq         bigint NOT NULL,
                type        text NOT NULL,
                payload     jsonb NOT NULL,
                PRIMARY KEY (workflow_id, seq)
            );
            CREATE TABLE IF NOT EXISTS tenaz_timers (
                workflow_id text NOT NULL REFERENCES tenaz_workflows (id) ON DELETE CASCADE,
                seq         int NOT NULL,
                fire_at     timestamptz NOT NULL,
                PRIMARY KEY (workflow_id, seq)
            );
            CREATE INDEX IF NOT EXISTS tenaz_timers_due ON tenaz_timers (fire_at);
            """;

    private static final Map<String, Class<? extends Event>> EVENT_TYPES = new HashMap<>();

    static {
        for (Class<?> type : Event.class.getPermittedSubclasses()) {
            EVENT_TYPES.put(type.getSimpleName(), type.asSubclass(Event.class));
        }
    }

    private final DataSource dataSource;
    private final ObjectMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
    private final Map<String, Set<Semaphore>> waiters = new ConcurrentHashMap<>();
    private final Thread listener;
    private volatile boolean closed;

    /** The data source should be a connection pool; one connection is kept for notifications. */
    public PostgresJournal(DataSource dataSource) {
        this.dataSource = dataSource;
        this.listener = Thread.ofPlatform().daemon().name("tenaz-pg-listener").start(this::listen);
    }

    /** Creates the tables if they do not exist. Safe to call from several processes at once. */
    public void migrate() {
        inTransaction(conn -> {
            try (Statement statement = conn.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(" + MIGRATION_LOCK + ")");
                statement.execute(SCHEMA);
            }
            return null;
        });
    }

    @Override
    public void close() {
        closed = true;
        listener.interrupt();
        try {
            listener.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean create(String workflowId, Event.WorkflowStarted started) {
        return inTransaction(conn -> {
            try (PreparedStatement insert = conn.prepareStatement(
                    "INSERT INTO tenaz_workflows (id, type) VALUES (?, ?) ON CONFLICT (id) DO NOTHING")) {
                insert.setString(1, workflowId);
                insert.setString(2, started.workflowType());
                if (insert.executeUpdate() == 0) {
                    return false;
                }
            }
            append(conn, workflowId, 0, List.of(started));
            return true;
        });
    }

    @Override
    public Optional<History> load(String workflowId) {
        // One statement, therefore one snapshot: status is derived from the events it returns.
        List<Event> events = inTransaction(conn -> {
            try (PreparedStatement select = conn.prepareStatement(
                    "SELECT type, payload FROM tenaz_events WHERE workflow_id = ? ORDER BY seq")) {
                select.setString(1, workflowId);
                List<Event> loaded = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        loaded.add(decode(rows.getString(1), rows.getString(2)));
                    }
                }
                return loaded;
            }
        });
        if (events.isEmpty()) {
            return Optional.empty();
        }
        String type = ((Event.WorkflowStarted) events.get(0)).workflowType();
        return Optional.of(new History(type, List.copyOf(events), statusAfter(events.get(events.size() - 1))));
    }

    @Override
    public Optional<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now) {
        return inTransaction(conn -> {
            try (PreparedStatement update = conn.prepareStatement("""
                    UPDATE tenaz_workflows
                       SET epoch = epoch + 1, lease_owner = ?, lease_expiry = ?
                     WHERE id = (
                           SELECT id FROM tenaz_workflows
                            WHERE status = 'RUNNING' AND type = ANY (?)
                              AND ((lease_expiry IS NULL AND version > processed_version) OR lease_expiry <= ?)
                            LIMIT 1
                              FOR UPDATE SKIP LOCKED)
                    RETURNING id, epoch
                    """)) {
                update.setString(1, workerId);
                update.setObject(2, timestamp(now.plus(ttl)));
                update.setArray(3, conn.createArrayOf("text", workflowTypes.toArray()));
                update.setObject(4, timestamp(now));
                try (ResultSet rows = update.executeQuery()) {
                    return rows.next()
                            ? Optional.of(new Lease(rows.getString(1), workerId, rows.getLong(2)))
                            : Optional.<Lease>empty();
                }
            }
        });
    }

    @Override
    public boolean renew(Lease lease, Duration ttl, Instant now) {
        return setExpiryIfHeld(lease, now.plus(ttl));
    }

    @Override
    public void abandon(Lease lease) {
        // An expiry in the past makes the workflow claimable at once while keeping it "leased",
        // which tells the next owner that there may be steps to run again.
        setExpiryIfHeld(lease, Instant.EPOCH);
    }

    private boolean setExpiryIfHeld(Lease lease, Instant expiry) {
        return inTransaction(conn -> {
            try (PreparedStatement update = conn.prepareStatement("UPDATE tenaz_workflows SET lease_expiry = ?"
                    + " WHERE id = ? AND epoch = ? AND status = 'RUNNING' AND lease_expiry IS NOT NULL")) {
                update.setObject(1, timestamp(expiry));
                update.setString(2, lease.workflowId());
                update.setLong(3, lease.epoch());
                return update.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void appendDecisions(Lease lease, long expectedVersion, List<Event> events) {
        inTransaction(conn -> {
            Row row = lockHeld(conn, lease);
            if (row.version != expectedVersion) {
                throw new VersionConflictException(lease.workflowId(), expectedVersion, row.version);
            }
            append(conn, lease.workflowId(), row.version, events);
            return null;
        });
    }

    @Override
    public void appendStepResult(Lease lease, Event event) {
        inTransaction(conn -> {
            Row row = lockHeld(conn, lease);
            append(conn, lease.workflowId(), row.version, List.of(event));
            return null;
        });
    }

    @Override
    public void appendExternal(String workflowId, Event event) {
        inTransaction(conn -> {
            Row row = lock(conn, workflowId);
            if (row == null) {
                throw new IllegalArgumentException("unknown workflow: " + workflowId);
            }
            if (row.running) {
                append(conn, workflowId, row.version, List.of(event));
            }
            return null;
        });
    }

    @Override
    public int fireDueTimers(Instant now) {
        return inTransaction(conn -> {
            record Due(String workflowId, int seq) {}
            List<Due> due = new ArrayList<>();
            try (PreparedStatement delete = conn.prepareStatement("""
                    DELETE FROM tenaz_timers
                     WHERE (workflow_id, seq) IN (
                           SELECT workflow_id, seq FROM tenaz_timers
                            WHERE fire_at <= ?
                            ORDER BY fire_at
                            LIMIT ?
                              FOR UPDATE SKIP LOCKED)
                    RETURNING workflow_id, seq
                    """)) {
                delete.setObject(1, timestamp(now));
                delete.setInt(2, TIMER_BATCH);
                try (ResultSet rows = delete.executeQuery()) {
                    while (rows.next()) {
                        due.add(new Due(rows.getString(1), rows.getInt(2)));
                    }
                }
            }
            // Workflow rows are locked in id order so that two engines firing timers at the same
            // time cannot deadlock on each other.
            due.sort(Comparator.comparing(Due::workflowId).thenComparing(Due::seq));
            int fired = 0;
            for (Due timer : due) {
                Row row = lock(conn, timer.workflowId());
                if (row != null && row.running) {
                    append(conn, timer.workflowId(), row.version, List.of(new Event.TimerFired(timer.seq())));
                    fired++;
                }
            }
            return fired;
        });
    }

    @Override
    public boolean park(Lease lease, long version) {
        return inTransaction(conn -> {
            Row row = lockHeld(conn, lease);
            if (row.version != version) {
                return false;
            }
            try (PreparedStatement update = conn.prepareStatement(
                    "UPDATE tenaz_workflows SET processed_version = ?, lease_expiry = NULL WHERE id = ?")) {
                update.setLong(1, version);
                update.setString(2, lease.workflowId());
                update.executeUpdate();
            }
            return true;
        });
    }

    @Override
    public boolean awaitChange(String workflowId, long version, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Semaphore wake = new Semaphore(0);
        waiters.compute(workflowId, (id, set) -> {
            Set<Semaphore> registered = set == null ? ConcurrentHashMap.newKeySet() : set;
            registered.add(wake);
            return registered;
        });
        try {
            // Registered before the first read, so a change after the read cannot be missed.
            while (true) {
                if (currentVersion(workflowId) > version) {
                    return true;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                wake.tryAcquire(Math.min(remaining, FALLBACK_POLL_NANOS), TimeUnit.NANOSECONDS);
            }
        } finally {
            waiters.compute(workflowId, (id, set) -> {
                set.remove(wake);
                return set.isEmpty() ? null : set;
            });
        }
    }

    private long currentVersion(String workflowId) {
        return inTransaction(conn -> {
            try (PreparedStatement select = conn.prepareStatement(
                    "SELECT version FROM tenaz_workflows WHERE id = ?")) {
                select.setString(1, workflowId);
                try (ResultSet rows = select.executeQuery()) {
                    if (!rows.next()) {
                        throw new IllegalArgumentException("unknown workflow: " + workflowId);
                    }
                    return rows.getLong(1);
                }
            }
        });
    }

    private record Row(boolean running, long version, long epoch, boolean leased) {}

    private Row lock(Connection conn, String workflowId) throws SQLException {
        try (PreparedStatement select = conn.prepareStatement(
                "SELECT status, version, epoch, lease_expiry IS NOT NULL FROM tenaz_workflows WHERE id = ? FOR UPDATE")) {
            select.setString(1, workflowId);
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                return new Row("RUNNING".equals(rows.getString(1)), rows.getLong(2), rows.getLong(3),
                        rows.getBoolean(4));
            }
        }
    }

    private Row lockHeld(Connection conn, Lease lease) throws SQLException {
        Row row = lock(conn, lease.workflowId());
        if (row == null || !row.running || !row.leased || row.epoch != lease.epoch()) {
            throw new FencedException(lease);
        }
        return row;
    }

    /** Appends to a workflow whose row the caller has locked. */
    private void append(Connection conn, String workflowId, long version, List<Event> events) throws SQLException {
        WorkflowStatus status = WorkflowStatus.RUNNING;
        try (PreparedStatement insertEvent = conn.prepareStatement(
                     "INSERT INTO tenaz_events (workflow_id, seq, type, payload) VALUES (?, ?, ?, ?::jsonb)");
             PreparedStatement insertTimer = conn.prepareStatement(
                     "INSERT INTO tenaz_timers (workflow_id, seq, fire_at) VALUES (?, ?, ?)")) {
            boolean timers = false;
            for (Event event : events) {
                insertEvent.setString(1, workflowId);
                insertEvent.setLong(2, version++);
                insertEvent.setString(3, event.getClass().getSimpleName());
                insertEvent.setString(4, encode(event));
                insertEvent.addBatch();
                if (event instanceof Event.TimerStarted timer) {
                    insertTimer.setString(1, workflowId);
                    insertTimer.setInt(2, timer.seq());
                    insertTimer.setObject(3, timestamp(timer.fireAt()));
                    insertTimer.addBatch();
                    timers = true;
                }
                if (statusAfter(event) != WorkflowStatus.RUNNING) {
                    status = statusAfter(event);
                }
            }
            insertEvent.executeBatch();
            if (timers) {
                insertTimer.executeBatch();
            }
        }
        try (PreparedStatement update = conn.prepareStatement("""
                UPDATE tenaz_workflows
                   SET version = ?, status = ?,
                       lease_expiry = CASE WHEN ? THEN NULL ELSE lease_expiry END
                 WHERE id = ?
                """)) {
            update.setLong(1, version);
            update.setString(2, status.name());
            update.setBoolean(3, status != WorkflowStatus.RUNNING);
            update.setString(4, workflowId);
            update.executeUpdate();
        }
        // Delivered when the transaction commits, and not at all if it rolls back.
        try (PreparedStatement notify = conn.prepareStatement("SELECT pg_notify(?, ?)")) {
            notify.setString(1, CHANNEL);
            notify.setString(2, workflowId);
            notify.execute();
        }
    }

    private static WorkflowStatus statusAfter(Event event) {
        return switch (event) {
            case Event.WorkflowCompleted ignored -> WorkflowStatus.COMPLETED;
            case Event.WorkflowFailed ignored -> WorkflowStatus.FAILED;
            default -> WorkflowStatus.RUNNING;
        };
    }

    private void listen() {
        while (!closed) {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(true);
                try (Statement statement = conn.createStatement()) {
                    statement.execute("LISTEN " + CHANNEL);
                }
                // Anything committed while we were not listening was not announced to us.
                waiters.values().forEach(set -> set.forEach(Semaphore::release));
                PGConnection pg = conn.unwrap(PGConnection.class);
                while (!closed && !Thread.currentThread().isInterrupted()) {
                    PGNotification[] notifications = pg.getNotifications(250);
                    if (notifications == null) {
                        continue;
                    }
                    for (PGNotification notification : notifications) {
                        Set<Semaphore> waiting = waiters.get(notification.getParameter());
                        if (waiting != null) {
                            waiting.forEach(Semaphore::release);
                        }
                    }
                }
            } catch (SQLException e) {
                if (closed) {
                    return;
                }
                LOG.log(System.Logger.Level.WARNING, "notification listener lost its connection; reconnecting", e);
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException interrupted) {
                    return;
                }
            }
        }
    }

    private String encode(Event event) {
        try {
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot encode " + event, e);
        }
    }

    private Event decode(String type, String payload) {
        Class<? extends Event> eventClass = EVENT_TYPES.get(type);
        if (eventClass == null) {
            throw new IllegalStateException("unknown event type in journal: " + type);
        }
        try {
            return mapper.readValue(payload, eventClass);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot decode " + type + " from " + payload, e);
        }
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @FunctionalInterface
    private interface Work<T> {
        T run(Connection conn) throws SQLException;
    }

    private <T> T inTransaction(Work<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                T result = work.run(conn);
                conn.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new JournalException("journal operation failed", e);
        }
    }

    public static final class JournalException extends RuntimeException {
        public JournalException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
