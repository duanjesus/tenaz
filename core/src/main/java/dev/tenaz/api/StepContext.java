package dev.tenaz.api;

/**
 * What a step knows about its own execution.
 *
 * @param idempotencyKey stable across retries and across workers; pass it to the system the step
 *                       calls so that a repeated execution has no second effect
 * @param attempt        1 for the first try within this worker
 */
public record StepContext(String idempotencyKey, int attempt) {}
