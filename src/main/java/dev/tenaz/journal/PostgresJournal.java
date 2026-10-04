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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * Journal backed by PostgreSQL.
 *
 * <p>An append is a single statement: it bumps the workflow row's version under the conditions
 * the caller is entitled to (the lease epoch, the expected version), and inserts the events and
 * timers only if that update matched. One round trip, atomic, and the row lock it takes
 * serializes writers of the same workflow. Claims use {@code FOR UPDATE SKIP LOCKED}, so workers
 * never queue behind each other. Changes are announced through {@code LISTEN/NOTIFY}, with
 * polling as the fallback in case a notification is lost with its connection.
 */
public final class PostgresJournal implements Journal, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(PostgresJournal.class.getName());
    private static final String CHANNEL = "tenaz_changes";
    private static final long MIGRATION_LOCK = 7_361_001L;
    private static final long FALLBACK_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final int TIMER_BATCH = 200;

    // "ready" marks an unowned workflow with events no owner has processed. Together with the
    // lease it defines the partial index, which therefore holds only workflows that are being
    // worked on or need to be: the ones asleep on a timer or a signal cost a claim nothing.
    private static final String SCHEMA = """
            CREATE TABLE IF NOT EXISTS tenaz_workflows (
                id           text PRIMARY KEY,
                type         text NOT NULL,
                status       text NOT NULL DEFAULT 'RUNNING',
                version      bigint NOT NULL DEFAULT 0,
                ready        boolean NOT NULL DEFAULT true,
                epoch        bigint NOT NULL DEFAULT 0,
                lease_owner  text,
                lease_expiry timestamptz
            );
            CREATE INDEX IF NOT EXISTS tenaz_workflows_claimable
                ON tenaz_workflows (type) WHERE status = 'RUNNING' AND (ready OR lease_expiry IS NOT NULL);
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

    private static final String LAST_SCHEMA_OBJECT = "tenaz_timers_due";

    private static final String CREATE = """
            WITH w AS (
                INSERT INTO tenaz_workflows (id, type, version) VALUES (?, ?, 1)
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            ), e AS (
                INSERT INTO tenaz_events (workflow_id, seq, type, payload)
                SELECT id, 0, ?, ?::jsonb FROM w
            )
            SELECT pg_notify('tenaz_changes', '1 t ' || id) FROM w
            """;

    private static final String APPEND = """
            WITH w AS (
                UPDATE tenaz_workflows
                   SET version = version + ?, status = ?, ready = true,
                       lease_expiry = CASE WHEN ? THEN NULL ELSE lease_expiry END
                 WHERE id = ? AND status = 'RUNNING'%s
             RETURNING id, version - ? AS base, version,
                       status = 'RUNNING' AND lease_expiry IS NULL AS claimable
            ), e AS (
                INSERT INTO tenaz_events (workflow_id, seq, type, payload)
                SELECT w.id, w.base + u.ord - 1, u.type, u.payload::jsonb
                  FROM w, unnest(?::text[], ?::text[]) WITH ORDINALITY AS u(type, payload, ord)
            ), t AS (
                INSERT INTO tenaz_timers (workflow_id, seq, fire_at)
                SELECT w.id, u.seq, u.fire_at::timestamptz
                  FROM w, unnest(?::int[], ?::text[]) AS u(seq, fire_at)
            )
            SELECT base, pg_notify('tenaz_changes', version || CASE WHEN claimable THEN ' t ' ELSE ' f ' END || id)
              FROM w
            """;
    private static final String APPEND_OWNED =
            APPEND.formatted(" AND epoch = ? AND lease_expiry IS NOT NULL AND version = ?");
    private static final String APPEND_EXTERNAL = APPEND.formatted("");

    // Takes the due timers, then locks their workflows in id order, so that two engines firing
    // timers of the same workflows cannot deadlock, then appends one TimerFired per timer.
    private static final String FIRE_TIMERS = """
            WITH due AS (
                DELETE FROM tenaz_timers
                 WHERE (workflow_id, seq) IN (
                       SELECT workflow_id, seq FROM tenaz_timers
                        WHERE fire_at <= ?
                        ORDER BY fire_at
                        LIMIT ?
                          FOR UPDATE SKIP LOCKED)
                RETURNING workflow_id, seq
            ), numbered AS (
                SELECT workflow_id, seq,
                       row_number() OVER (PARTITION BY workflow_id ORDER BY seq) AS nth,
                       count(*) OVER (PARTITION BY workflow_id) AS total
                  FROM due
            ), locked AS (
                SELECT id FROM tenaz_workflows
                 WHERE id IN (SELECT workflow_id FROM due) AND status = 'RUNNING'
                 ORDER BY id
                   FOR UPDATE
            ), w AS (
                UPDATE tenaz_workflows w
                   SET version = w.version + c.total, ready = true
                  FROM locked, (SELECT DISTINCT workflow_id, total FROM numbered) c
                 WHERE w.id = locked.id AND w.id = c.workflow_id
             RETURNING w.id, w.version - c.total AS base, w.version, c.total, w.lease_expiry IS NULL AS claimable
            ), e AS (
                INSERT INTO tenaz_events (workflow_id, seq, type, payload)
                SELECT w.id, w.base + n.nth - 1, 'TimerFired', jsonb_build_object('seq', n.seq)
                  FROM numbered n JOIN w ON w.id = n.workflow_id
            )
            SELECT total, pg_notify('tenaz_changes', version || CASE WHEN claimable THEN ' t ' ELSE ' f ' END || id)
              FROM w
            """;

    private static final String CLAIM = """
            WITH candidates AS MATERIALIZED (
                SELECT id FROM tenaz_workflows
                 WHERE status = 'RUNNING' AND (ready OR lease_expiry IS NOT NULL)
                   AND type = ANY (?)
                   AND (lease_expiry IS NULL OR lease_expiry <= ?)
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
            )
            UPDATE tenaz_workflows w
               SET epoch = epoch + 1, lease_owner = ?, lease_expiry = ?
              FROM candidates
             WHERE w.id = candidates.id
            RETURNING w.id, w.epoch
            """;

    private static final String RENEW = """
            UPDATE tenaz_workflows w
               SET lease_expiry = ?
              FROM unnest(?::text[], ?::bigint[]) AS held(id, epoch)
             WHERE w.id = held.id AND w.epoch = held.epoch
               AND w.status = 'RUNNING' AND w.lease_expiry IS NOT NULL
            RETURNING w.id, w.epoch
            """;

    private static final String HELD =
            " WHERE id = ? AND epoch = ? AND status = 'RUNNING' AND lease_expiry IS NOT NULL";

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
    private final List<ChangeListener> listeners = new CopyOnWriteArrayList<>();
    private final Thread listener;
    private volatile boolean closed;

    /** The data source should be a connection pool; one connection is kept for notifications. */
    public PostgresJournal(DataSource dataSource) {
        this.dataSource = dataSource;
        this.listener = Thread.ofPlatform().daemon().name("tenaz-pg-listener").start(this::listen);
    }

    /**
     * Creates the tables if they do not exist. Safe to call from several processes at once, and
     * while engines are running: when the schema is already there it touches nothing, because
     * even a skipped {@code CREATE INDEX IF NOT EXISTS} takes a table lock that can deadlock
     * with the engines' own statements.
     */
    public void migrate() {
        inTransaction(conn -> {
            try (Statement statement = conn.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(" + MIGRATION_LOCK + ")");
                try (ResultSet rows = statement.executeQuery("SELECT to_regclass('" + LAST_SCHEMA_OBJECT + "')")) {
                    rows.next();
                    if (rows.getString(1) != null) {
                        return null;
                    }
                }
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
        return execute(conn -> {
            try (PreparedStatement insert = conn.prepareStatement(CREATE)) {
                insert.setString(1, workflowId);
                insert.setString(2, started.workflowType());
                insert.setString(3, started.getClass().getSimpleName());
                insert.setString(4, encode(started));
                try (ResultSet rows = insert.executeQuery()) {
                    return rows.next();
                }
            }
        });
    }

    @Override
    public Optional<History> load(String workflowId) {
        // One statement, therefore one snapshot: status is derived from the events it returns.
        List<Event> events = loadSince(workflowId, 0);
        if (events.isEmpty()) {
            return Optional.empty();
        }
        String type = ((Event.WorkflowStarted) events.get(0)).workflowType();
        return Optional.of(new History(type, events, statusAfter(events.get(events.size() - 1))));
    }

    @Override
    public List<Event> loadSince(String workflowId, long fromVersion) {
        return execute(conn -> {
            try (PreparedStatement select = conn.prepareStatement(
                    "SELECT type, payload FROM tenaz_events WHERE workflow_id = ? AND seq >= ? ORDER BY seq")) {
                select.setString(1, workflowId);
                select.setLong(2, fromVersion);
                List<Event> loaded = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        loaded.add(decode(rows.getString(1), rows.getString(2)));
                    }
                }
                return List.copyOf(loaded);
            }
        });
    }

    @Override
    public List<Lease> claim(String workerId, Set<String> workflowTypes, Duration ttl, Instant now, int limit) {
        return execute(conn -> {
            try (PreparedStatement update = conn.prepareStatement(CLAIM)) {
                update.setArray(1, conn.createArrayOf("text", workflowTypes.toArray()));
                update.setObject(2, timestamp(now));
                update.setInt(3, limit);
                update.setString(4, workerId);
                update.setObject(5, timestamp(now.plus(ttl)));
                List<Lease> claimed = new ArrayList<>();
                try (ResultSet rows = update.executeQuery()) {
                    while (rows.next()) {
                        claimed.add(new Lease(rows.getString(1), workerId, rows.getLong(2)));
                    }
                }
                return claimed;
            }
        });
    }

    @Override
    public Set<Lease> renew(Collection<Lease> leases, Duration ttl, Instant now) {
        if (leases.isEmpty()) {
            return Set.of();
        }
        Map<String, Map<Long, Lease>> byWorkflow = new HashMap<>();
        for (Lease lease : leases) {
            byWorkflow.computeIfAbsent(lease.workflowId(), id -> new HashMap<>()).put(lease.epoch(), lease);
        }
        return execute(conn -> {
            try (PreparedStatement update = conn.prepareStatement(RENEW)) {
                update.setObject(1, timestamp(now.plus(ttl)));
                update.setArray(2, conn.createArrayOf("text", leases.stream().map(Lease::workflowId).toArray()));
                update.setArray(3, conn.createArrayOf("int8", leases.stream().map(Lease::epoch).toArray()));
                Set<Lease> held = new LinkedHashSet<>();
                try (ResultSet rows = update.executeQuery()) {
                    while (rows.next()) {
                        held.add(byWorkflow.get(rows.getString(1)).get(rows.getLong(2)));
                    }
                }
                return held;
            }
        });
    }

    @Override
    public void abandon(Lease lease) {
        // An expiry in the past makes the workflow claimable at once while keeping it "leased",
        // which tells the next owner that there may be steps to run again.
        execute(conn -> {
            try (PreparedStatement update = conn.prepareStatement(
                    "UPDATE tenaz_workflows SET lease_expiry = ?" + HELD)) {
                update.setObject(1, timestamp(Instant.EPOCH));
                update.setString(2, lease.workflowId());
                update.setLong(3, lease.epoch());
                return update.executeUpdate();
            }
        });
    }

    @Override
    public void append(Lease lease, long expectedVersion, List<Event> events) {
        execute(conn -> {
            if (!append(conn, lease.workflowId(), lease, expectedVersion, events)) {
                State state = state(conn, lease.workflowId());
                if (state == null || !state.heldBy(lease)) {
                    throw new FencedException(lease);
                }
                throw new VersionConflictException(lease.workflowId(), expectedVersion, state.version);
            }
            return null;
        });
    }

    @Override
    public void appendExternal(String workflowId, Event event) {
        execute(conn -> {
            if (!append(conn, workflowId, null, 0, List.of(event)) && state(conn, workflowId) == null) {
                throw new IllegalArgumentException("unknown workflow: " + workflowId);
            }
            return null;
        });
    }

    @Override
    public int fireDueTimers(Instant now) {
        return execute(conn -> {
            try (PreparedStatement fire = conn.prepareStatement(FIRE_TIMERS)) {
                fire.setObject(1, timestamp(now));
                fire.setInt(2, TIMER_BATCH);
                int fired = 0;
                try (ResultSet rows = fire.executeQuery()) {
                    while (rows.next()) {
                        fired += rows.getInt(1);
                    }
                }
                return fired;
            }
        });
    }

    @Override
    public boolean park(Lease lease, long version) {
        return execute(conn -> {
            try (PreparedStatement update = conn.prepareStatement(
                    "UPDATE tenaz_workflows SET ready = false, lease_expiry = NULL" + HELD + " AND version = ?")) {
                update.setString(1, lease.workflowId());
                update.setLong(2, lease.epoch());
                update.setLong(3, version);
                if (update.executeUpdate() == 1) {
                    return true;
                }
            }
            State state = state(conn, lease.workflowId());
            if (state == null || !state.heldBy(lease)) {
                throw new FencedException(lease);
            }
            return false;
        });
    }

    @Override
    public Runnable subscribe(ChangeListener listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
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
                State state = execute(conn -> state(conn, workflowId));
                if (state == null) {
                    throw new IllegalArgumentException("unknown workflow: " + workflowId);
                }
                if (state.version > version) {
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

    private record State(boolean running, long version, long epoch, boolean leased) {
        boolean heldBy(Lease lease) {
            return running && leased && epoch == lease.epoch();
        }
    }

    private State state(Connection conn, String workflowId) throws SQLException {
        try (PreparedStatement select = conn.prepareStatement(
                "SELECT status, version, epoch, lease_expiry IS NOT NULL FROM tenaz_workflows WHERE id = ?")) {
            select.setString(1, workflowId);
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                return new State("RUNNING".equals(rows.getString(1)), rows.getLong(2), rows.getLong(3),
                        rows.getBoolean(4));
            }
        }
    }

    /**
     * Appends events in one statement. With a lease, the append happens only if the lease is
     * still held and the history is at the expected version. Returns whether it happened.
     */
    private boolean append(Connection conn, String workflowId, Lease lease, long expectedVersion,
                           List<Event> events) throws SQLException {
        WorkflowStatus status = WorkflowStatus.RUNNING;
        String[] types = new String[events.size()];
        String[] payloads = new String[events.size()];
        List<Integer> timerSeqs = new ArrayList<>();
        List<String> timerFireAts = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            types[i] = event.getClass().getSimpleName();
            payloads[i] = encode(event);
            if (event instanceof Event.TimerStarted timer) {
                timerSeqs.add(timer.seq());
                timerFireAts.add(timer.fireAt().toString());
            }
            if (statusAfter(event) != WorkflowStatus.RUNNING) {
                status = statusAfter(event);
            }
        }
        try (PreparedStatement statement = conn.prepareStatement(lease != null ? APPEND_OWNED : APPEND_EXTERNAL)) {
            int p = 1;
            statement.setInt(p++, events.size());
            statement.setString(p++, status.name());
            statement.setBoolean(p++, status != WorkflowStatus.RUNNING);
            statement.setString(p++, workflowId);
            if (lease != null) {
                statement.setLong(p++, lease.epoch());
                statement.setLong(p++, expectedVersion);
            }
            statement.setInt(p++, events.size());
            statement.setArray(p++, conn.createArrayOf("text", types));
            statement.setArray(p++, conn.createArrayOf("text", payloads));
            statement.setArray(p++, conn.createArrayOf("int4", timerSeqs.toArray()));
            statement.setArray(p, conn.createArrayOf("text", timerFireAts.toArray()));
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
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
                        announce(notification.getParameter());
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

    /** @param payload the new version, whether the workflow is claimable (t or f), and its id */
    private void announce(String payload) {
        int space = payload.indexOf(' ');
        long version = Long.parseLong(payload, 0, space, 10);
        boolean claimable = payload.charAt(space + 1) == 't';
        String workflowId = payload.substring(space + 3);
        Set<Semaphore> waiting = waiters.get(workflowId);
        if (waiting != null) {
            waiting.forEach(Semaphore::release);
        }
        listeners.forEach(subscriber -> subscriber.changed(workflowId, version, claimable));
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

    /** Runs single statements, each of which commits on its own. */
    private <T> T execute(Work<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            return work.run(conn);
        } catch (SQLException e) {
            throw new JournalException("journal operation failed", e);
        }
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
