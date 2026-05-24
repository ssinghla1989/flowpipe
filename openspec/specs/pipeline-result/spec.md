# pipeline-result

## Purpose

Defines the sealed `Result<O>` returned from `Pipeline.execute`, its `Success<O>` and `Failure<O>` record variants, the structure of the accompanying `ExecutionTrace`/`TraceEntry`, and the rule that a step throwing or failing validation terminates the pipeline as a `Failure` carrying the failing step id — so callers handle outcomes by pattern-matching variants rather than by try/catch.

## Requirements

### Requirement: Result is a sealed two-variant type

The library SHALL expose `Result<O>` as a sealed interface permitting exactly two record implementations: `Success<O>` and `Failure<O>`. `Pipeline.execute(...)` SHALL return a `Result<O>`. Callers MUST be able to discriminate the two variants using `instanceof` pattern matching (Java 17+) without casts. Consumers on Java 21+ obtain switch-expression exhaustiveness for free as a consequence of the sealing; this is not a Java-17 requirement.

#### Scenario: Pattern-matching instanceof handles both arms without a cast

- **WHEN** a developer writes `if (result instanceof Success<O> s) { ... s.value() ... } else if (result instanceof Failure<O> f) { ... f.failedStepId() ... }` on Java 17
- **THEN** the Java compiler MUST accept both pattern bindings without requiring any cast, and the two arms MUST cover every permitted subtype of `Result<O>`

### Requirement: Success carries the produced value and an execution trace

`Success<O>` SHALL be a record with components `O value()` and `ExecutionTrace trace()`. A successful execution SHALL return a `Success<O>` whose `value()` equals the final step's output and whose `trace()` lists one `TraceEntry` per executed step in the order executed.

#### Scenario: Successful execution produces Success with correct value

- **WHEN** a two-step pipeline `stepA -> stepB` executes successfully and `stepB` returns `42`
- **THEN** the result MUST be a `Success<Integer>` with `value() == 42` and `trace()` containing two `TraceEntry` entries with `stepId` values matching `stepA` and `stepB` in that order

### Requirement: Failure carries the cause, the failing step id, and a partial trace

`Failure<O>` SHALL be a record with components `Throwable cause()`, `String failedStepId()`, and `ExecutionTrace trace()`. `failedStepId()` MUST equal the id of the step whose execution or validation produced the failure. `trace()` MUST contain `TraceEntry` entries for every step that ran before the failing step plus a final entry for the failing step itself.

#### Scenario: Failure identifies the failing step

- **WHEN** a three-step pipeline `stepA -> stepB -> stepC` executes and `stepB` throws
- **THEN** the result MUST be a `Failure`, `failedStepId()` MUST equal `stepB`'s id, `trace()` MUST contain two `TraceEntry` entries (for `stepA` and `stepB`), and `stepC` MUST NOT appear in the trace

### Requirement: ExecutionTrace exposes per-step timing and attempt count

`ExecutionTrace` SHALL be a value that exposes an ordered list of `TraceEntry`. Each `TraceEntry` SHALL be a record with at minimum the components `String stepId()`, `long startedAtNanos()`, `long durationNanos()`, `int attempts()`, and `boolean skipped()`. For entries representing executed steps (sequential, parallel, or branch-arm taken steps), `attempts()` MUST be at least `1` and `skipped()` MUST return `false`. For entries representing non-taken branch arm steps or the branch node's own synthetic entry, `skipped()` indicates whether the step was bypassed. The branch node's synthetic `TraceEntry` (with `stepId` equal to the branch id) records the predicate evaluation time as `durationNanos`, `attempts = 1`, and `skipped() == false`.

In the baseline (no retry configured), executed steps SHALL have `attempts() == 1`; later changes that add retry SHALL populate it with the real attempt count. A SKIPPED entry SHALL have `durationNanos == 0` and `attempts == 0`.

#### Scenario: Trace entry for an executed step reports timing and a single attempt

- **WHEN** a step completes successfully in a pipeline with no retry policy configured
- **THEN** its `TraceEntry.durationNanos()` MUST be greater than or equal to `0`, `startedAtNanos()` MUST be the wall-clock-monotonic start time captured by the engine, `attempts()` MUST equal `1`, and `skipped()` MUST return `false`

#### Scenario: SKIPPED trace entry has zero duration and zero attempts

- **WHEN** a pipeline with a branch node executes and a step in the non-taken arm produces a SKIPPED trace entry
- **THEN** that entry's `durationNanos()` MUST equal `0`, `attempts()` MUST equal `0`, and `skipped()` MUST return `true`

#### Scenario: Branch node synthetic trace entry appears in trace with branch id

- **WHEN** a pipeline with a branch node whose `branchId` is `"route"` executes
- **THEN** the `ExecutionTrace` MUST contain a `TraceEntry` with `stepId() == "route"`, `attempts() == 1`, `skipped() == false`, and `durationNanos()` reflecting the predicate evaluation time
