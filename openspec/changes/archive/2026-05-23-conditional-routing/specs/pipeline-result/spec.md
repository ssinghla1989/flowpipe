## MODIFIED Requirements

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
