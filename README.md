# Tenaz

A durable execution engine for Java 21. You write a business process as ordinary Java; Tenaz
guarantees it runs to completion even if every machine running it dies along the way.

```java
engine.register("order", Order.class, String.class, (ctx, order) -> {
    String payment = ctx.step("charge", String.class, step ->
            payments.charge(order, step.idempotencyKey()));

    DurablePromise<String> approval = ctx.signal("approve", String.class);
    DurablePromise<Void> deadline = ctx.timer(Duration.ofDays(3));   // holds no thread

    if (ctx.anyOf(approval, deadline) == deadline) {
        ctx.run("refund", step -> payments.refund(payment, step.idempotencyKey()));
        return "expired";
    }
    return ctx.step("ship", String.class, step -> warehouse.ship(order));
});
```

If the process is killed between `charge` and `ship`, another engine picks the order up and
continues from where it stopped. `charge` is not called again, and the three-day timer does not
restart.

## How it works

**The journal is the state.** Every workflow has an append-only history of events
(`StepScheduled`, `StepCompleted`, `TimerFired`, `SignalReceived`, ...). Nothing else is persisted:
no stack, no variables.

**State is rebuilt by replay.** To advance a workflow, the engine runs its code from the top
against the history. Calls the history already answers return the recorded answer; the first call
it cannot answer unwinds the code. New commands are journaled, steps are run, and the code is
replayed again when anything changes. See [Replay.java](src/main/java/dev/tenaz/engine/Replay.java).

**Replay is deterministic by construction.** Workflow code can observe a pending result only by
waiting on it, and `anyOf` picks its winner by position in the history, never by wall-clock
arrival. A replay against a longer history therefore always retraces a replay against a shorter
one. Clocks, randomness and UUIDs go through the context and are recorded or derived. Code that
diverges from its own history is detected and rejected (`NonDeterminismError`).

**One owner at a time, enforced by fencing.** An engine drives a workflow under a lease. Each
claim bumps the workflow's epoch, and every write carries the epoch it was made under, so a worker
that lost its lease (crash, long GC pause, partition) cannot write: the journal rejects it.
Decisions additionally use optimistic concurrency on the history version, so a signal that lands
mid-replay forces a fresh replay instead of being ordered inconsistently.

## Guarantees

| | |
|---|---|
| Workflow progress | Runs to completion as long as one engine is alive |
| Journaled step | Never executed again |
| Step in flight during a crash | Executed again: **at-least-once**, with a stable idempotency key to make its effect exactly-once |
| Timers | Survive restarts; fire once |
| Starting a workflow | Idempotent on the workflow id |

## Evidence

[ChaosTest](src/test/java/dev/tenaz/ChaosTest.java) runs 300 money transfers on a three-node
cluster while killing a random node 60 times, and asserts that every transfer completes and every
debit and credit takes effect exactly once. A typical run forces about 40 steps to execute twice;
the balances still match to the cent.

```
./mvnw test
```

## Limitations

- The only journal is in-memory: it survives engine crashes, not the JVM. A PostgreSQL journal is
  the next milestone.
- Retry attempts are counted per worker; a takeover restarts the count.
- Payload types are plain classes (`Class<T>`); generic types such as `List<Foo>` are not supported.
- No workflow versioning: changing the code of a workflow with executions in flight fails them.
- `finally` blocks in workflow code run on every replay.

## Roadmap

1. PostgreSQL journal (`SKIP LOCKED` claims, `LISTEN/NOTIFY` wake-ups), crash tests with `kill -9`
2. Deterministic simulation testing: simulated clock, scheduler and faults under one seed
3. Child workflows, cancellation, versioning
4. Spring Boot starter and a history viewer
5. Benchmarks
