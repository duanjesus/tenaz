package dev.tenaz.spring;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("tenaz")
public class TenazProperties {

    public enum JournalType {
        /** PostgreSQL if the application has a PostgreSQL data source, in memory otherwise. */
        AUTO,
        POSTGRES,
        MEMORY
    }

    /** Where workflow histories are kept. */
    private JournalType journal = JournalType.AUTO;

    /** Whether to create the tenaz_* tables at startup if they do not exist. */
    private boolean migrate = true;

    /** Whether this application runs workflows, or only starts and signals them. */
    private boolean workersEnabled = true;

    /** Name of this engine in leases; a random one by default. */
    private String workerId;

    /** How long a dead engine's workflows stay stuck before another engine takes them over. */
    private Duration leaseTtl = Duration.ofSeconds(10);

    /** How often to look for work that no notification announced, such as expired leases. */
    private Duration pollInterval = Duration.ofMillis(50);

    /** How many workflows this engine drives at the same time. */
    private int maxConcurrentWorkflows = 1000;

    public JournalType getJournal() {
        return journal;
    }

    public void setJournal(JournalType journal) {
        this.journal = journal;
    }

    public boolean isMigrate() {
        return migrate;
    }

    public void setMigrate(boolean migrate) {
        this.migrate = migrate;
    }

    public boolean isWorkersEnabled() {
        return workersEnabled;
    }

    public void setWorkersEnabled(boolean workersEnabled) {
        this.workersEnabled = workersEnabled;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Duration getLeaseTtl() {
        return leaseTtl;
    }

    public void setLeaseTtl(Duration leaseTtl) {
        this.leaseTtl = leaseTtl;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getMaxConcurrentWorkflows() {
        return maxConcurrentWorkflows;
    }

    public void setMaxConcurrentWorkflows(int maxConcurrentWorkflows) {
        this.maxConcurrentWorkflows = maxConcurrentWorkflows;
    }
}
