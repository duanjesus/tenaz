package dev.tenaz.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.tenaz.api.JacksonCodec;
import dev.tenaz.api.PayloadCodec;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.InMemoryJournal;
import dev.tenaz.journal.Journal;
import dev.tenaz.journal.JournalBrowser;
import dev.tenaz.journal.PostgresJournal;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ClassUtils;

/**
 * Gives the application a {@link TenazEngine} that knows every {@link DurableWorkflow} bean.
 * Each bean here steps aside if the application defines its own.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration"})
@EnableConfigurationProperties(TenazProperties.class)
public class TenazAutoConfiguration {

    /** Static, as a registry post-processor has to exist before the beans it is going to add. */
    @Bean
    static DurableWorkflowScanner tenazWorkflowScanner() {
        return new DurableWorkflowScanner();
    }

    /** Payloads are encoded with the application's own ObjectMapper, so its modules apply. */
    @Bean
    @ConditionalOnMissingBean
    PayloadCodec tenazPayloadCodec(ObjectProvider<ObjectMapper> mapper) {
        ObjectMapper applicationMapper = mapper.getIfAvailable();
        return applicationMapper != null ? new JacksonCodec(applicationMapper) : new JacksonCodec();
    }

    @Bean
    @ConditionalOnMissingBean
    Journal tenazJournal(TenazProperties properties, ObjectProvider<DataSource> dataSources) {
        DataSource dataSource = dataSources.getIfAvailable();
        boolean postgres = switch (properties.getJournal()) {
            case MEMORY -> false;
            case POSTGRES -> true;
            case AUTO -> dataSource != null && isPostgres(dataSource);
        };
        if (!postgres) {
            return new InMemoryJournal();
        }
        if (dataSource == null) {
            throw new IllegalStateException("tenaz.journal=postgres needs a DataSource, and the application has none");
        }
        return Postgres.journal(dataSource, properties.isMigrate());
    }

    @Bean
    @ConditionalOnMissingBean
    TenazEngine tenazEngine(Journal journal, PayloadCodec codec, TenazProperties properties) {
        TenazEngine.Builder builder = TenazEngine.builder(journal)
                .codec(codec)
                .leaseTtl(properties.getLeaseTtl())
                .pollInterval(properties.getPollInterval())
                .maxConcurrentWorkflows(properties.getMaxConcurrentWorkflows());
        if (properties.getRetention() != null) {
            builder.retention(properties.getRetention());
        }
        if (properties.getWorkerId() != null) {
            builder.workerId(properties.getWorkerId());
        }
        return builder.build();
    }

    @Bean
    TenazWorkflowRegistrar tenazWorkflowRegistrar(TenazEngine engine, TenazProperties properties,
                                                  ListableBeanFactory beans) {
        return new TenazWorkflowRegistrar(engine, properties, beans);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnProperty(name = "tenaz.viewer.enabled", havingValue = "true")
    static class Viewer {

        @Bean
        @ConditionalOnMissingBean
        TenazViewerController tenazViewer(Journal journal) {
            if (!(journal instanceof JournalBrowser browser)) {
                throw new IllegalStateException("tenaz.viewer.enabled needs a journal that can be browsed, and "
                        + journal.getClass().getName() + " does not implement " + JournalBrowser.class.getName());
            }
            return new TenazViewerController(browser);
        }
    }

    private static boolean isPostgres(DataSource dataSource) {
        if (!ClassUtils.isPresent("org.postgresql.Driver", TenazAutoConfiguration.class.getClassLoader())) {
            return false;
        }
        try (Connection connection = dataSource.getConnection()) {
            return "PostgreSQL".equals(connection.getMetaData().getDatabaseProductName());
        } catch (SQLException e) {
            throw new IllegalStateException("could not ask the DataSource which database it is", e);
        }
    }

    /** Kept apart so that PostgresJournal is only loaded when the PostgreSQL driver is there. */
    private static final class Postgres {
        static Journal journal(DataSource dataSource, boolean migrate) {
            PostgresJournal journal = new PostgresJournal(dataSource);
            if (migrate) {
                journal.migrate();
            }
            return journal;
        }
    }
}
