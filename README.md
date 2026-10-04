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

**The engine never touches the world directly.** It is a set of short, non-blocking tasks that get
time, scheduling and step execution from an
[EngineRuntime](src/main/java/dev/tenaz/engine/EngineRuntime.java). In production that is virtual
threads and the wall clock; under test it is a single-threaded event loop driven by a seed.

## Guarantees

| | |
|---|---|
| Workflow progress | Runs to completion as long as one engine is alive |
| Journaled step | Never executed again |
| Step in flight during a crash | Executed again: **at-least-once**, with a stable idempotency key to make its effect exactly-once |
| Timers | Survive restarts; fire once |
| Starting a workflow | Idempotent on the workflow id |

## Evidence

**Deterministic simulation.** [SimulationTest](src/test/java/dev/tenaz/sim/SimulationTest.java)
runs a three-node cluster inside one thread, with simulated time and every source of randomness
drawn from one seed. For thirty simulated seconds, nodes crash and restart, freeze for longer than
their leases and wake up as zombies, run on skewed clocks, lose notifications, and see journal
operations fail both before and after committing. Then the faults stop and the run must converge:
every workflow finished, every effect applied exactly once, every history well-formed.

- 500 seeds run on every build in about a second; 30,000 seeds (257 hours of simulated time,
  290,000 crashes, 920,000 journal failures) run in under a minute and pass.
- A failing seed fails identically every time: `./mvnw test -Dtest=SimulationTest -Dtenaz.sim.seed=16`.
- The simulator is itself tested for the ability to fail. With fencing removed from the journal,
  it finds the resulting double write. With `anyOf` changed to prefer its first argument over
  history order, it reports the broken workflow at seed 16.

**Real processes.** The same test suites run against both journals.

- [ChaosTest](src/test/java/dev/tenaz/ChaosTest.java) runs 300 money transfers on a three-node
  cluster while killing a random node 60 times, and asserts that every transfer completes and every
  debit and credit takes effect exactly once. A typical run forces 30 to 70 steps to execute twice;
  the balances still match to the cent.
- [KillNineTest](src/test/java/dev/tenaz/KillNineTest.java) simulates nothing: the workers are
  separate JVMs on PostgreSQL, and the operating system destroys one every second or so, with no
  chance to clean up. Same assertions, same result.

```
./mvnw test
```

The PostgreSQL tests start a container and are skipped when Docker is not available.

## Using PostgreSQL

```java
PostgresJournal journal = new PostgresJournal(dataSource);   // a pooled DataSource
journal.migrate();                                            // creates the tenaz_* tables
TenazEngine engine = TenazEngine.builder(journal).build();
```

An append is a single statement. It bumps the workflow row's version only under the conditions the
caller is entitled to (the lease epoch, the expected version), and inserts the events and timers
only if that update matched: one round trip, atomic, with the row lock serializing writers of the
same workflow. A step therefore costs one commit, which carries its outcome together with whatever
the workflow decided to do next. Workers claim work in batches with `FOR UPDATE SKIP LOCKED` and
are woken by `LISTEN/NOTIFY`, with polling as a fallback. See
[PostgresJournal.java](src/main/java/dev/tenaz/journal/PostgresJournal.java).

## Benchmarks

Steps do nothing, so these numbers are the engine's own overhead: what it costs to make a step
durable. Measured on a laptop (Core i7-11370H, 16 GB, Windows 11) with PostgreSQL 17 in Docker
Desktop at default settings, `fsync` on. On this setup `pgbench -N` commits 700 transactions/s
with one client and 4,300 with 32, which is the ceiling for anything that commits per step. Runs
on this machine vary a lot, so each figure is the range of two runs.

| Journal | Scenario | Result |
|---|---|---|
| PostgreSQL | 1 engine, 3,000 workflows of 5 steps | 280 to 440 workflows/s (1,400 to 2,200 steps/s) |
| PostgreSQL | 3 engines, 3,000 workflows of 5 steps | 325 to 470 workflows/s (1,600 to 2,400 steps/s) |
| PostgreSQL | idle engine, one 5-step workflow at a time | p50 35 ms, p99 90 to 115 ms |
| PostgreSQL | one workflow of 1,000 sequential steps | 400 to 540 steps/s |
| in-memory | 1 engine, 20,000 workflows of 5 steps | 16,000 to 19,000 workflows/s |
| in-memory | one workflow of 5,000 sequential steps | 1,200 to 1,700 steps/s |

Reading them:

- A 5-step workflow costs 8 commits (create, claim, first command, one per step), so 440
  workflows/s is about 3,500 commits/s: the engine is close to what this database can commit, and
  adding engines adds little because the database, not the engine, is the limit.
- Latency on an idle engine is those same 8 commits back to back.
- The in-memory figures show what the engine costs without a database: about 10 microseconds per
  step for short workflows.
- Long workflows are slower per step because every step replays the workflow code from the top.

The work in this milestone was measured against the commit before it with the same harness:
throughput went from 85-103 to 280-440 workflows/s, the 1,000-step workflow from 46-66 to 400-540
steps/s, and idle p50 latency from 82-101 ms to 35 ms. The gains came from keeping each
workflow's history in its session and fetching only new events, making an append one statement
instead of a five-statement transaction, journaling a step's outcome together with the next
command, and claiming and renewing leases in batches. A partial index keeps sleeping workflows out
of the claim path: with 200,000 of them in the table, a claim reads only the 50 rows that need
work.

```
./mvnw -q test-compile exec:java -Dexec.mainClass=dev.tenaz.bench.Benchmark -Dexec.classpathScope=test
```

## Limitations

- Every step replays the workflow code from the top, so CPU cost grows with the square of a
  workflow's length: fine for hundreds of steps, slow for many thousands.
- The benchmarks are from one laptop with the database in a local container. They say nothing
  about a real server or a network between engine and database.
- Retry attempts are counted per worker; a takeover restarts the count.
- Payload types are plain classes (`Class<T>`); generic types such as `List<Foo>` are not supported.
- No workflow versioning: changing the code of a workflow with executions in flight fails them.
- `finally` blocks in workflow code run on every replay.
- Signals are not deduplicated: a client that retries a signal after an ambiguous failure may
  deliver it twice.
- The simulation covers the engine on the in-memory journal. `PostgresJournal` is covered by the
  contract, chaos and kill -9 tests instead.

## Roadmap

1. Keep the workflow's stack alive between steps instead of replaying from the top
2. Child workflows, cancellation, versioning
3. Spring Boot starter and a history viewer
