## MODIFIED Requirements

### Requirement: Engine exposes a MetricsRecorder SPI with a no-op default

The library SHALL define a `MetricsRecorder` interface in `io.flowpipe.observability` with exactly three methods: `recordStepDuration(String stepId, long durationNanos)`, `recordStepAttempts(String stepId, int attempts)`, and `recordStepOutcome(String stepId, StepOutcome outcome)` where `StepOutcome` is an enum with values `SUCCESS`, `FAILURE`, and `SKIPPED`. The library SHALL ship `NoOpMetricsRecorder` as a stateless singleton accessible via a static `instance()` accessor whose methods do nothing.

#### Scenario: NoOpMetricsRecorder methods are no-ops

- **WHEN** any of `NoOpMetricsRecorder.instance().recordStepDuration("x", 1L)`, `recordStepAttempts("x", 1)`, or `recordStepOutcome("x", StepOutcome.SUCCESS)` is invoked
- **THEN** the call MUST return normally without throwing and MUST have no observable side effect

#### Scenario: NoOpMetricsRecorder handles SKIPPED outcome without throwing

- **WHEN** `NoOpMetricsRecorder.instance().recordStepOutcome("x", StepOutcome.SKIPPED)` is invoked
- **THEN** the call MUST return normally without throwing

## ADDED Requirements

### Requirement: Engine emits a step.skip log event for non-taken branch arm steps

For each step in the non-taken arm of a branch node, the engine SHALL emit one SLF4J log event at `DEBUG` level with the message `step.skip` and at minimum these structured key-value fields: `step.id` (the step's descriptor id) and `step.branch_id` (the id of the enclosing branch node). No `step.start`, `step.finish`, or `step.error` event SHALL be emitted for skipped steps.

#### Scenario: step.skip log is emitted at DEBUG for each step in the non-taken arm

- **WHEN** a branch node with id `"route"` selects `ifTrue` and `ifFalse` contains steps `"step-c"` and `"step-d"`
- **THEN** the captured log events MUST contain exactly two events with message `step.skip` and level `DEBUG`, one for `step.id="step-c"` and one for `step.id="step-d"`, each with `step.branch_id="route"`
- **AND** no `step.start`, `step.finish`, or `step.error` events MUST be emitted for `"step-c"` or `"step-d"`

### Requirement: Engine invokes MetricsRecorder with SKIPPED outcome for non-taken branch arm steps

For each step in the non-taken arm of a branch node, after appending the SKIPPED `TraceEntry`, the engine SHALL invoke the configured recorder's `recordStepDuration(stepId, 0L)`, `recordStepAttempts(stepId, 0)`, and `recordStepOutcome(stepId, StepOutcome.SKIPPED)` exactly once.

#### Scenario: SKIPPED steps invoke recorder with SKIPPED outcome and zero values

- **WHEN** a branch node selects `ifTrue` and `ifFalse` contains a single step `"skip-me"`, and the pipeline is executed against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one duration event for `"skip-me"` with value `0L`, one attempts event for `"skip-me"` with value `0`, and one outcome event for `"skip-me"` with value `SKIPPED`
