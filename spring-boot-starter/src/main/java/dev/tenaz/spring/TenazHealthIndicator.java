package dev.tenaz.spring;

import dev.tenaz.engine.TenazEngine;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Down when the engine cannot reach its journal, or when its workers have stopped renewing
 * their leases: an engine in that state is about to lose its workflows to others.
 */
public class TenazHealthIndicator implements HealthIndicator {

    private static final String PROBE_ID = "tenaz-health-probe";

    private final TenazEngine engine;
    private final boolean workersEnabled;
    private final Clock clock;

    public TenazHealthIndicator(TenazEngine engine, boolean workersEnabled) {
        this(engine, workersEnabled, Clock.systemUTC());
    }

    TenazHealthIndicator(TenazEngine engine, boolean workersEnabled, Clock clock) {
        this.engine = engine;
        this.workersEnabled = workersEnabled;
        this.clock = clock;
    }

    @Override
    public Health health() {
        Health.Builder health = Health.up()
                .withDetail("journal", engine.journal().getClass().getSimpleName())
                .withDetail("activeWorkflows", engine.activeWorkflows());
        try {
            engine.journal().started(PROBE_ID);
        } catch (RuntimeException e) {
            return health.down(e).build();
        }
        if (!workersEnabled) {
            return health.withDetail("workers", "disabled").build();
        }
        Optional<Instant> renewed = engine.lastLeaseRenewal();
        if (renewed.isEmpty()) {
            return health.withDetail("leases", "not renewed yet").build();
        }
        Duration age = Duration.between(renewed.get(), clock.instant());
        health.withDetail("lastLeaseRenewal", age.toMillis() + " ms ago");
        if (age.compareTo(engine.leaseTtl()) > 0) {
            return health.down().withDetail("reason", "leases have not been renewed for longer than their "
                    + engine.leaseTtl().toMillis() + " ms lifetime").build();
        }
        return health.build();
    }
}
