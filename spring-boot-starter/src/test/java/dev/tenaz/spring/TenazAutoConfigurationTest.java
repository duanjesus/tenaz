package dev.tenaz.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowContext;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import dev.tenaz.journal.PostgresJournal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

class TenazAutoConfigurationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    static class Greeter {
        String greet(String name) {
            return "hello " + name;
        }
    }

    @DurableWorkflow("greet")
    static class GreetingWorkflow implements Workflow<String, String> {
        private final Greeter greeter;

        GreetingWorkflow(Greeter greeter) {
            this.greeter = greeter;
        }

        @Override
        public String run(WorkflowContext ctx, String name) {
            return ctx.step("greet", String.class, step -> greeter.greet(name));
        }
    }

    @DurableWorkflow("impostor")
    static class NotAWorkflow {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, TenazAutoConfiguration.class))
            .withPropertyValues("tenaz.poll-interval=5ms")
            .withBean(Greeter.class)
            .withBean(GreetingWorkflow.class);

    @Test
    void workflowBeansAreRegisteredAndRunWithTheirCollaborators() {
        runner.run(context -> {
            assertThat(context).getBean(Journal.class).isInstanceOf(InMemoryJournal.class);
            TenazEngine engine = context.getBean(TenazEngine.class);

            assertThat(engine.<String>start("greet", "greet-1", "ana").result(TIMEOUT)).isEqualTo("hello ana");
        });
    }

    @Test
    void anApplicationWithWorkersDisabledOnlyStartsWorkflows() {
        runner.withPropertyValues("tenaz.workers-enabled=false").run(context -> {
            WorkflowHandle<String> handle = context.getBean(TenazEngine.class).start("greet", "greet-2", "bia");
            Thread.sleep(200);

            assertThat(handle.isDone()).isFalse();
        });
    }

    @Test
    void theApplicationCanSupplyItsOwnJournal() {
        InMemoryJournal own = new InMemoryJournal();
        runner.withBean(Journal.class, () -> own).run(context -> {
            context.getBean(TenazEngine.class).start("greet", "greet-3", "caio").result(TIMEOUT);

            assertThat(own.load("greet-3")).isPresent();
        });
    }

    @Test
    void anAnnotatedBeanThatIsNotAWorkflowStopsTheApplicationFromStarting() {
        runner.withBean(NotAWorkflow.class).run(context ->
                assertThat(context).getFailure().hasStackTraceContaining("does not implement"));
    }

    @Test
    void aPostgresDataSourceMakesTheJournalDurable() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
        try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")) {
            postgres.start();
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(postgres.getJdbcUrl());
            config.setUsername(postgres.getUsername());
            config.setPassword(postgres.getPassword());
            try (HikariDataSource dataSource = new HikariDataSource(config)) {
                // The context closes the data source with everything else, so look inside it.
                runner.withBean(DataSource.class, () -> dataSource).run(context -> {
                    assertThat(context).getBean(Journal.class).isInstanceOf(PostgresJournal.class);
                    TenazEngine engine = context.getBean(TenazEngine.class);

                    assertThat(engine.<String>start("greet", "greet-4", "duda").result(TIMEOUT))
                            .isEqualTo("hello duda");
                    try (Connection connection = dataSource.getConnection();
                         ResultSet rows = connection.createStatement().executeQuery(
                                 "SELECT status FROM tenaz_workflows WHERE id = 'greet-4'")) {
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getString(1)).isEqualTo("COMPLETED");
                    }
                });
            }
        }
    }
}
