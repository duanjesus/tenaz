package dev.tenaz;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.tenaz.journal.PostgresJournal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/** One PostgreSQL container shared by every test class in the JVM. Tests are skipped without Docker. */
public final class PostgresTestSupport {

    private static PostgreSQLContainer<?> container;
    private static HikariDataSource dataSource;

    private PostgresTestSupport() {}

    public static synchronized PostgreSQLContainer<?> container() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        if (container == null) {
            container = new PostgreSQLContainer<>("postgres:17-alpine");
            container.start();
        }
        return container;
    }

    public static synchronized DataSource dataSource() {
        if (dataSource == null) {
            dataSource = pool(container().getJdbcUrl(), container().getUsername(), container().getPassword(), 20);
        }
        return dataSource;
    }

    public static HikariDataSource pool(String url, String user, String password, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(size);
        return new HikariDataSource(config);
    }

    /** A journal over empty tables. The caller closes it. */
    public static PostgresJournal freshJournal() {
        PostgresJournal journal = new PostgresJournal(dataSource());
        journal.migrate();
        // A statement of an engine that the previous test crashed may still be running in the
        // database, and TRUNCATE takes its locks in a different order than that statement does.
        for (int attempt = 1; ; attempt++) {
            try {
                execute("TRUNCATE tenaz_workflows CASCADE");
                return journal;
            } catch (IllegalStateException e) {
                if (attempt == 5) {
                    throw e;
                }
            }
        }
    }

    public static void execute(String sql) {
        try (Connection conn = dataSource().getConnection(); Statement statement = conn.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
