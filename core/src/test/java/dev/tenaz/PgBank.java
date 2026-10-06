package dev.tenaz;

import dev.tenaz.ChaosTest.Transfer;
import dev.tenaz.api.Workflow;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import javax.sql.DataSource;

/**
 * A bank that lives in PostgreSQL, so that its state outlives the worker processes that the
 * kill -9 test destroys. Like a real payment API, it deduplicates on the idempotency key.
 */
final class PgBank {

    private final DataSource dataSource;

    PgBank(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    void reset(int accounts, long openingBalance) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement statement = conn.createStatement()) {
            statement.execute("""
                    DROP TABLE IF EXISTS bank_accounts, bank_applied, bank_executions;
                    CREATE TABLE bank_accounts (id text PRIMARY KEY, balance bigint NOT NULL);
                    CREATE TABLE bank_applied (key text PRIMARY KEY);
                    CREATE TABLE bank_executions (key text NOT NULL);
                    """);
            statement.execute("INSERT INTO bank_accounts SELECT 'acc-' || n, " + openingBalance
                    + " FROM generate_series(0, " + (accounts - 1) + ") n");
        }
    }

    void move(String idempotencyKey, String account, long delta) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement log = conn.prepareStatement("INSERT INTO bank_executions VALUES (?)");
                 PreparedStatement apply = conn.prepareStatement(
                         "INSERT INTO bank_applied VALUES (?) ON CONFLICT DO NOTHING");
                 PreparedStatement update = conn.prepareStatement(
                         "UPDATE bank_accounts SET balance = balance + ? WHERE id = ?")) {
                log.setString(1, idempotencyKey);
                log.executeUpdate();
                apply.setString(1, idempotencyKey);
                if (apply.executeUpdate() == 1) {
                    update.setLong(1, delta);
                    update.setString(2, account);
                    update.executeUpdate();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    long query(String sql) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            rows.next();
            return rows.getLong(1);
        }
    }

    Workflow<Transfer, String> transferWorkflow() {
        return (ctx, t) -> {
            ctx.run("debit", step -> {
                move(step.idempotencyKey(), t.from(), -t.amount());
                // The window in which a kill leaves an effect applied but not journaled.
                Thread.sleep(ThreadLocalRandom.current().nextInt(40));
            });
            ctx.sleep(Duration.ofMillis(50));
            ctx.run("credit", step -> {
                move(step.idempotencyKey(), t.to(), t.amount());
                Thread.sleep(ThreadLocalRandom.current().nextInt(40));
            });
            return "ok";
        };
    }
}
