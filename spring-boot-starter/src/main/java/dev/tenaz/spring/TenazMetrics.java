package dev.tenaz.spring;

import dev.tenaz.engine.EngineObserver;
import dev.tenaz.engine.TenazEngine;
import dev.tenaz.journal.Journal.WorkflowStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Publishes what the engine does as Micrometer meters.
 *
 * <ul>
 *   <li>{@code tenaz.workflows.active}: workflows this engine is driving now</li>
 *   <li>{@code tenaz.workflows.claimed}: workflows it took ownership of</li>
 *   <li>{@code tenaz.workflows.ended}, by {@code type} and {@code status}</li>
 *   <li>{@code tenaz.step.attempts}, by {@code workflow}, {@code step} and {@code outcome}:
 *       how many attempts and how long they took</li>
 *   <li>{@code tenaz.leases.lost}: workflows found to belong to another engine. It should stay
 *       flat while engines are healthy</li>
 *   <li>{@code tenaz.leases.renewal} and {@code tenaz.journal.append}: how long the journal takes</li>
 * </ul>
 */
public class TenazMetrics implements EngineObserver, SmartInitializingSingleton {

    private final MeterRegistry registry;
    private final ObjectProvider<TenazEngine> engine;

    public TenazMetrics(MeterRegistry registry, ObjectProvider<TenazEngine> engine) {
        this.registry = registry;
        this.engine = engine;
    }

    /** The gauge needs the engine, and the engine needs this observer: so the gauge comes last. */
    @Override
    public void afterSingletonsInstantiated() {
        TenazEngine running = engine.getIfAvailable();
        if (running != null) {
            Gauge.builder("tenaz.workflows.active", running, TenazEngine::activeWorkflows)
                    .description("Workflows this engine is driving now")
                    .register(registry);
        }
    }

    @Override
    public void workflowsClaimed(int count) {
        registry.counter("tenaz.workflows.claimed").increment(count);
    }

    @Override
    public void workflowEnded(String workflowType, WorkflowStatus status) {
        registry.counter("tenaz.workflows.ended", "type", workflowType, "status", status.name().toLowerCase())
                .increment();
    }

    @Override
    public void stepAttempted(String workflowType, String stepName, StepOutcome outcome, Duration took) {
        registry.timer("tenaz.step.attempts", "workflow", workflowType, "step", stepName,
                "outcome", outcome.name().toLowerCase()).record(took);
    }

    @Override
    public void leaseLost(String workflowId) {
        registry.counter("tenaz.leases.lost").increment();
    }

    @Override
    public void leasesRenewed(int count, Duration took) {
        registry.timer("tenaz.leases.renewal").record(took);
    }

    @Override
    public void journalAppended(int events, Duration took) {
        registry.timer("tenaz.journal.append").record(took);
    }
}
