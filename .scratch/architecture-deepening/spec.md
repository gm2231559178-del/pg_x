# Spec: Deepen the edges the architecture review missed

Status: ready-for-agent

## Problem Statement

The first architecture pass deepened three modules behind narrow, testable seams:
the **session loop** (reconnection/backoff/shutdown), **delivery** (one handle per
transport), and the **replication protocol** (the whole wire format behind
`ReplicationProtocol<S>`). A second pass over the untouched edges found the same
shallowness still living in the hot paths, plus two live correctness bugs:

- `pgx replicate --where "…status='active'" --sink postgres …` **applies every
  row to the Postgres target**. The `--table` / `--op` / `--where` filters gate
  only the fan-out sinks; the applier is fed unconditionally, so filtered-out
  rows still land in the replica. The documented filter contract is silently
  false for the `postgres` sink.
- To-many GraphQL resolvers keyed by a **numeric** parameter return empty
  relations with no error. The batch-key round-trip is split across three
  modules with three different conversion rules; the lookup path only matches
  strings, so a key stored as `"42"` is looked up as `None`.

Beyond the two bugs, the surrounding modules are shallow in the same way the
refactored ones were: untested pure logic (the `--where` parser), policies
repeated at the call site (the lenient/strict error decision, the LSN-advance
rule), and a broker consumer seam whose failure channel hides disconnection from
the session loop.

## Solution

Deepen the six edges in priority order. Each is independently shippable; the
first two fix live correctness bugs and should land first.

1. **ReplicateSession + applier gate.** Give the replicate command the same
   shape the consume session has: one `ReplicateSession` owning a run behind
   `new` + `run`, the filter decision evaluated once per event and applied to
   *both* the applier and the sinks, and the LSN-advance policy moved inside the
   session.
2. **One batch-key codec.** One injective, symmetric `batch_key()` used by the
   loader and the lookup paths, and one resolver dispatch path (batch or not)
   with one missing-resolver policy, so numeric keys stop losing children.
3. **Grammar vs semantics in the row filter.** Split the `--where` parser into
   a grammar layer (AST with SQL precedence) and an evaluate layer (documented
   three-valued NULL semantics); one shared table-prefix grammar; the config +
   CLI merge becomes constructors.
4. **The applier owns txn boundaries and a health story.** A pure SQL layer
   that is unit-tested (skip paths included), `apply(&[WalEvent])` so a source
   WAL txn commits to the target as one unit, and a dead-target path that
   escalates to the session loop instead of silently growing a buffer.
5. **One error policy for the consume session.** The lenient/strict decision is
   one method with a documented requeue / discard / abort matrix, and an
   effective config is derived once so no code compares against literal clap
   defaults.
6. **A real failure channel in the consumer interface.** `recv()` distinguishes
   a clean end from an error, so broker errors reach the session-loop reconnect;
   a typed delivery `Tag`; one shared message-id helper tested for stability.

## User Stories

1. As a replicate operator, I want `--table`, `--op`, and `--where` filters to
   gate the Postgres target as well as the fan-out sinks, so that filtered-out
   rows never land in the replica.
2. As a replicate operator, I want the row-filter decision to be identical no
   matter which sink I chose, so that filtering semantics do not depend on the
   downstream transport.
3. As a developer, I want the replicate run owned by a session behind a `new` +
   `run` interface, so that the event loop is testable without a live Postgres.
4. As a developer, I want the LSN-advance policy (applier active vs not) to live
   inside the session, so that reconnection and progress behavior are covered by
   tests.
5. As a replicate operator, I want an applier failure to surface as a session
   outcome, so that retry/give-up is decided once instead of inside a nested
   inline loop.
6. As a compose user, I want to-many resolvers keyed by a numeric parameter to
   return their children, so that relations do not silently come back empty.
7. As a compose user, I want the batch-key round-trip to be symmetric for
   string, numeric, and boolean values, so that any key type survives
   load → resolve.
8. As a developer, I want the GraphQL executor testable without a live
   Postgres, so that resolver dispatch is covered by unit tests.
9. As a developer, I want one missing-resolver policy shared by batch and
   non-batch paths, so that the two modes cannot diverge again.
10. As a replicate operator, I want `--where "a OR b AND c"` to mean
    `a OR (b AND c)`, so that filter precedence matches SQL.
11. As a replicate operator, I want `--where "amount = 100"` to match a stored
    `"100.0"`, so that numeric comparisons behave consistently.
12. As a replicate operator, I want `--where "col != NULL"` to be false, so
    that NULL semantics match SQL.
13. As a developer, I want one shared table-prefix grammar, so that the row
    filter and the transforms cannot drift on `schema.table:` parsing.
14. As a developer, I want the config + CLI merge for drop-columns and renames
    to be constructors, so that the global-vs-specific ordering is deterministic
    and testable.
15. As a replicate operator, I want the target transaction boundaries to match
    the source WAL transaction boundaries, so that a crash mid-transaction does
    not replay partial data as duplicate inserts.
16. As a replicate operator, I want a dead target to fail the session into
    reconnect, so that the applier buffer does not grow without bound.
17. As a developer, I want the SQL generators to be unit-tested, including their
    skip paths, so that "SKIPPED" rows are an explicit, verified decision.
18. As a developer, I want the applier's buffer lifecycle and error recovery to
    be one testable unit, so that flush policy is not split across the session
    and the applier.
19. As a consume operator, I want a transient sink failure in lenient mode to be
    requeued rather than silently discarded, so that no durable event is dropped
    because of a hiccup.
20. As a developer, I want the lenient/strict decision to be one method with a
    documented matrix, so that the four duplicated inline matches collapse.
21. As a developer, I want the effective consume config derived once, so that
    changing a clap default cannot silently break config merging.
22. As a developer, I want the consume session testable through a scripted
    consumer, so that error-path transitions are covered through `run()`.
23. As a Kafka consume operator, I want broker errors to reach the session-loop
    reconnect, so that a dead consumer escalates instead of retrying every
    second forever.
24. As a developer, I want the delivery tag round-trip typed per broker, so that
    the opaque encoding convention stops leaking into the interface.
25. As a consume operator running `--idempotent`, I want message identity to be
    stable across brokers, so that two distinct RabbitMQ messages with identical
    bodies are not falsely deduped.

## Implementation Decisions

Each decision names the deepened module, its interface, and the policy that
moves behind it. No file paths — modules are named by concept.

### Priority 1 — ReplicateSession and the applier gate

- **D1.1 ReplicateSession.** A session for the replicate command mirroring the
  consume session: `ReplicateSession::new(row_filter, transforms, applier,
  sink, resume_lsn) -> run(&mut self, replication client, shutdown) ->
  SessionExit`. It owns the relation cache and the resume-LSN, and it returns
  the existing `SessionExit` outcomes so the session-loop factory is unchanged.
- **D1.2 One filter decision.** The `should_forward` decision is evaluated once
  per decoded event and gates *both* the applier and the sinks. The decision is
  computed on the pre-transform event; transforms apply afterward (documented
  and asserted, not incidental).
- **D1.3 LSN-advance policy inside the session.** With an applier active, LSN
  advances at commit boundaries; without one, it advances per `XLogData`. The
  policy is a session method, unit-testable with a scripted client.
- **D1.4 Applier failures as session outcomes.** A failed `handle_event` no
  longer `continue`s deep in the event loop; it is surfaced so the session
  decides between reconnect, retry, or fatal, consistent with the session loop.

### Priority 2 — One batch-key codec

- **D2.1 `batch_key()`.** A single function serializing a batch parameter value
  (string, numeric, boolean) to a key, injective and symmetric. The loader and
  the lookup paths both call it — the invariant lives in one place with tests.
- **D2.2 One resolver dispatch.** The three repeated dispatch paths collapse
  into one resolver parameterized by batch mode, with one missing-resolver
  policy (error or warn, same in both modes).
- **D2.3 A query-runner interface.** The concrete pool reference threaded
  through resolver signatures is replaced by a narrow async interface for
  running a composed document, so the executor is driven by an in-memory fake in
  tests — the same adapter trick the consume session uses for composition.

### Priority 3 — Grammar vs semantics in the row filter

- **D3.1 Parser produces an AST.** The `--where` grammar becomes a tokenizer +
  parser with table-driven precedence, so `AND` binds tighter than `OR` and the
  AST is inspectable. This is a pure function of the filter string.
- **D3.2 Evaluator owns semantics.** A separate pure layer evaluates the AST
  against a row with a documented three-valued NULL table: comparisons with
  NULL are NULL (never true), and `Neq(NULL)` is false. Numeric comparison
  parses both sides as numbers when both parse, else falls back to string.
- **D3.3 One table-prefix grammar.** The `schema.table:` split is a single
  shared helper used by both the row filter and the transforms; the duplicated
  copy is deleted.
- **D3.4 Constructors own the merge.** `RowFilter::from_sources` and
  `ColumnTransforms::from_sources` assemble config + CLI inputs, including the
  deterministic ordering rule for a global rule vs a specific rule, so the
  merge leaves the session and is testable with plain strings.

### Priority 4 — The applier owns boundaries and health

- **D4.1 A pure SQL layer.** The INSERT / UPDATE / DELETE / TRUNCATE generators
  become an exposed, unit-tested layer including their skip ("SKIPPED") paths,
  so a skipped row is an explicit, verified decision rather than silent data
  loss.
- **D4.2 `apply(&[WalEvent])`.** The applier's unit of work is a source WAL
  transaction. Flush-on-full and the buffer lifecycle live behind this method
  so the target transaction boundaries equal the source boundaries.
- **D4.3 A health story.** A dead target no longer logs-and-continues while the
  buffer grows. A failed apply on a dead target surfaces as a session outcome
  that reaches the session-loop reconnect path, mirroring the source client.

### Priority 5 — One error policy for the consume session

- **D5.1 `ErrorMode::handle`.** The four inline lenient/strict matches collapse
  into one method taking the failure stage and returning a control-flow
  decision (requeue / discard / abort). The matrix is documented in one place:
  lenient requeues transient sink failures instead of discarding.
- **D5.2 Effective config derived once.** An effective config is produced by
  explicit merge logic; no code compares against literal clap defaults, so
  changing a `default_value` cannot silently break merging.

### Priority 6 — A real failure channel in the consumer interface

- **D6.1 `recv()` distinguishes failure.** `recv()` returns a clean-end vs an
  error outcome, and the default-true `is_connected` goes away in favor of the
  explicit outcome, so broker errors reach the session-loop reconnect.
- **D6.2 A typed delivery `Tag`.** The opaque delivery tag becomes a newtype
  with per-broker encode/decode, so the packed `(partition, offset)` convention
  stops leaking into the interface.
- **D6.3 One message-id helper.** A shared helper derives stable message
  identity from the broker's native key/property, tested for stability across
  redelivery — replacing the payload-hash fallback that can falsely dedupe two
  distinct identical-bodied messages.

## Testing Decisions

- **Good tests assert external behavior through the seam** — they drive the
  session / executor / parser / applier through its interface and assert the
  outcome, never reach into implementation. The suite already models this:
  the replication protocol is scripted over `tokio::io::duplex` peers, and the
  session loop is driven by a fake factory. The new tests follow that pattern.
- **ReplicateSession** is tested by feeding scripted decoded events through
  `run()` with in-memory fakes for the applier and sinks, asserting: which
  events reach each downstream (the gate), the LSN-advance policy in both
  applier-active and applier-inactive modes, and the session outcome on applier
  failure. Prior art: the session-loop tests and the scripted replication
  client tests.
- **batch_key / the executor** is tested as a pure symmetry suite (number,
  string, bool round-trip load → resolve) plus executor behavior against a fake
  query runner, asserting the missing-resolver policy and batch vs non-batch
  equivalence. Prior art: `lsn.rs` pure round-trip tests.
- **The row filter** is tested as pure parser + evaluator suites over string
  inputs: precedence (`a OR b AND c`), NULL semantics, numeric vs string
  comparison, and the shared table-prefix splitter. The constructors are tested
  for global-vs-specific merge ordering. Prior art: config merge tests and the
  replication pure-function tests.
- **The SQL layer and applier** are tested in two layers: generators as pure
  string tests (including skip paths), and `apply(&[WalEvent])` against a fake
  statement executor to assert txn-boundary preservation and the dead-target
  outcome. Prior art: the scripted in-memory peer pattern.
- **The consume error policy** is tested as a pure matrix: each failure stage ×
  lenient/strict maps to the right requeue/discard/abort outcome, plus the
  effective-config derivation. Prior art: `config.rs` merge tests.
- **The consumer interface** is tested by driving the consume session with a
  scripted fake consumer that returns the error outcome, asserting the session
  escalates to reconnect; the `Tag` and message-id helpers get pure
  round-trip/stability tests. Prior art: the scripted replication client.

## Out of Scope

- Changing the already-deepened interfaces (session loop, delivery handles,
  replication protocol) — their seams are stable and reused as-is.
- New sinks, new brokers, or new filter operators.
- Executor performance or query-planning changes beyond the dispatch collapse.
- Broader CLI ergonomics (e.g. renaming flags) or config-format changes.
- Anything in the sibling commands not named above (`export`, `info`, `query`,
  `graphql`, `psql`, `profiles`, `doctor`, `mcp`).

## Further Notes

- **Ordering:** filter decision is computed on the original event, transforms
  apply afterward. This ordering is now explicit and tested; changing it is a
  separate decision.
- **Sequencing:** each feature is independently shippable and verifiable
  against the current test baseline (115 tests). Land 1 and 2 first — they are
  live correctness bugs — then 3–6 in the listed order.
- **Numeric comparison:** where both sides parse as numbers, compare
  numerically; otherwise compare as strings. The previous behavior (raw string
  equality alongside numeric inequality) is the bug being fixed, not a contract.
- **Consumer reconnect escalation:** the existing session-loop reconnect
  (backoff, give-up, shutdown) is reused — the consumer work is about making
  broker errors *reach* it, not building a new loop.
- **Skip semantics:** "SKIPPED" rows are a deliberate, documented behavior (a
  row that cannot be applied without a key); the tests make it visible rather
  than silent.
