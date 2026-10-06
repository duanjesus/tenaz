# Changelog

## Unreleased

- Retention: `TenazEngine.Builder.retention` and `tenaz.retention` delete a workflow and its
  history a set time after it ends. PostgreSQL gains an `ended_at` column, added by `migrate()`.

## 0.1.1 (2026-10-06)

- Licensed under the MIT License. No code changes since 0.1.0.

## 0.1.0 (2026-10-06)

First release.

### Engine

- Workflows as plain Java: steps with retries, parallel steps, durable timers, signals, `anyOf`.
- Child workflows, with the child's outcome delivered to the parent atomically.
- Cancellation, delivered to workflow code at a point decided by the history and passed on to
  the children the workflow is waiting for.
- `ctx.version` for changing workflow code under executions in flight.
- Lease-based ownership with fencing epochs; workflow code kept alive between steps and rebuilt
  by replay when another engine takes over.

### Journals

- In-memory journal for tests.
- PostgreSQL journal: single-statement appends, batched claims with `FOR UPDATE SKIP LOCKED`,
  `LISTEN/NOTIFY` wake-ups.

### Spring Boot

- `tenaz-spring-boot-starter`: auto-configured engine, `@DurableWorkflow` beans, `tenaz.*`
  properties.
- Optional read-only history viewer at `/tenaz`.
- An example order service.

### Evidence

- Deterministic simulation of a three-node cluster under crashes, freezes, clock skew, lost
  notifications and journal failures, reproducible by seed.
- Chaos tests on both journals and a test that kills real worker processes.
- Benchmarks, with their limits, in the README.

### Known limitations

See the Limitations section of the README. Most important: no clean-up of finished workflows,
signals are not deduplicated, steps have no timeout, and the simulation does not cover the
PostgreSQL journal.
