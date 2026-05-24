## ADDED Requirements

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

`ExecutionTrace` SHALL be a value that exposes an ordered list of `TraceEntry`. Each `TraceEntry` SHALL be a record with at least the components `String stepId()`, `long startedAtNanos()`, `long durationNanos()`, and `int attempts()`. In this slice, `attempts()` MUST always equal `1`; later changes that add retry SHALL populate it with the real attempt count.

#### Scenario: Trace entry reports timing and a single attempt

- **WHEN** a step completes successfully in a pipeline with no retry policy configured
- **THEN** its `TraceEntry.durationNanos()` MUST be greater than or equal to `0`, `startedAtNanos()` MUST be the wall-clock-monotonic start time captured by the engine, and `attempts()` MUST equal `1`
