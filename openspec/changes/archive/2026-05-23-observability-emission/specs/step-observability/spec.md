## ADDED Requirements

### Requirement: Engine emits a structured log event at the start of every step

For each step invocation, the engine SHALL emit one SLF4J log event at `INFO` level with the message `step.start` and at minimum these structured key-value fields: `step.id` (the step's descriptor id) and `step.attempt` (always `1` in this slice). The engine SHALL also include every `RequestContext` entry as a structured key-value pair whose key equals the `ContextKey.name()` and whose value is the entry's value. The log event SHALL be emitted before input validation runs.

#### Scenario: step.start log is emitted with step id and context fields

- **WHEN** a pipeline containing a step with id `enrich` is executed with a `RequestContext` carrying `ContextKey<String> TRACE_ID` ("traceId") mapped to `"abc-123"` and the SLF4J backend is configured to capture structured key-value pairs
- **THEN** the captured log events MUST contain exactly one event whose message is `step.start` and whose structured fields include `step.id="enrich"`, `step.attempt=1`, and `traceId="abc-123"`

### Requirement: Engine emits a structured log event when a step finishes successfully

When `step.execute` returns successfully and output validation passes, the engine SHALL emit one SLF4J log event at `INFO` level with the message `step.finish` and at minimum these structured key-value fields: `step.id`, `step.attempt`, `step.duration_ms` (the elapsed wall-clock time of the step invocation, converted from the same `nanoTime`-derived value used to build the corresponding `TraceEntry`), and `step.outcome="success"`. The engine SHALL also include every `RequestContext` entry as a structured key-value pair as described in the start-event requirement.

#### Scenario: step.finish log is emitted with duration and success outcome

- **WHEN** a pipeline whose single step has id `compute` completes successfully
- **THEN** the captured log events MUST contain exactly one event whose message is `step.finish` and whose structured fields include `step.id="compute"`, `step.outcome="success"`, and a `step.duration_ms` value greater than or equal to `0`

### Requirement: Engine emits a structured log event when a step fails

When a step's input validation, `execute`, or output validation throws any `Throwable`, the engine SHALL emit one SLF4J log event at `ERROR` level with the message `step.error` and at minimum these structured key-value fields: `step.id`, `step.attempt`, `step.duration_ms`, `step.outcome="failure"`, `step.error_class` (the throwable's class name), and `step.error_message` (the throwable's message, or the empty string if `null`). The engine SHALL also include every `RequestContext` entry as a structured key-value pair as described in the start-event requirement. No `step.finish` event SHALL be emitted for the failing step.

#### Scenario: step.error log is emitted with error class and message

- **WHEN** a pipeline contains a step with id `unstable` whose `execute` throws `new IllegalStateException("nope")`
- **THEN** the captured log events MUST contain exactly one event whose message is `step.error`, whose level is `ERROR`, and whose structured fields include `step.id="unstable"`, `step.outcome="failure"`, `step.error_class` ending with `IllegalStateException`, and `step.error_message="nope"`
- **AND** no event with message `step.finish` MUST be emitted for `step.id="unstable"`

### Requirement: Engine exposes a MetricsRecorder SPI with a no-op default

The library SHALL define a `MetricsRecorder` interface in `io.flowpipe.observability` with exactly three methods: `recordStepDuration(String stepId, long durationNanos)`, `recordStepAttempts(String stepId, int attempts)`, and `recordStepOutcome(String stepId, StepOutcome outcome)` where `StepOutcome` is an enum with values `SUCCESS` and `FAILURE`. The library SHALL ship `NoOpMetricsRecorder` as a stateless singleton accessible via a static `instance()` accessor whose methods do nothing.

#### Scenario: NoOpMetricsRecorder methods are no-ops

- **WHEN** any of `NoOpMetricsRecorder.instance().recordStepDuration("x", 1L)`, `recordStepAttempts("x", 1)`, or `recordStepOutcome("x", StepOutcome.SUCCESS)` is invoked
- **THEN** the call MUST return normally without throwing and MUST have no observable side effect

### Requirement: PipelineBuilder accepts a build-time MetricsRecorder default

`PipelineBuilder<I, O>` SHALL expose a `withMetrics(MetricsRecorder recorder)` method that returns the same builder (or a chainable replacement) and that records the supplied recorder as the default for the pipeline being built. If `withMetrics(...)` is not called, the built pipeline's default SHALL be `NoOpMetricsRecorder.instance()`. Calling `withMetrics(...)` more than once SHALL replace the previously-configured recorder with the new one; no merging or chaining SHALL occur. Passing `null` SHALL throw `NullPointerException`.

#### Scenario: builder default is the no-op recorder when withMetrics is not called

- **WHEN** a pipeline is built without calling `.withMetrics(...)` and executed
- **THEN** the engine MUST invoke no observable methods on any non-no-op recorder

#### Scenario: withMetrics replaces a previously-configured recorder

- **WHEN** a developer calls `.withMetrics(recorderA).withMetrics(recorderB)` and the pipeline is executed
- **THEN** recorderB MUST receive the emission calls and recorderA MUST receive none

### Requirement: Pipeline.execute supports a per-call MetricsRecorder override

`Pipeline<I, O>` SHALL expose an overload `execute(I input, RequestContext context, MetricsRecorder recorder)` that uses the supplied `recorder` for that single invocation, overriding the build-time default. The existing `execute(I)` and `execute(I, RequestContext)` overloads SHALL continue to use the build-time default. The override SHALL NOT mutate the pipeline's build-time default; subsequent calls without the override SHALL again use the build-time default.

#### Scenario: per-call recorder receives emissions, pipeline default stays unchanged

- **WHEN** a pipeline configured with `.withMetrics(defaultRecorder)` is executed first with `execute(input, ctx, overrideRecorder)` and then with `execute(input, ctx)`
- **THEN** `overrideRecorder` MUST receive emissions only from the first call, and `defaultRecorder` MUST receive emissions only from the second call

### Requirement: Engine invokes MetricsRecorder around every step on both success and failure paths

For each step invocation, after the `TraceEntry` is appended, the engine SHALL invoke the configured recorder's `recordStepDuration(stepId, durationNanos)`, `recordStepAttempts(stepId, 1)`, and `recordStepOutcome(stepId, outcome)` exactly once, where `outcome` is `SUCCESS` on the success path and `FAILURE` on any error path. The `durationNanos` value SHALL equal the `durationNanos` field on the corresponding `TraceEntry`.

#### Scenario: success path invokes recorder with SUCCESS outcome

- **WHEN** a pipeline with a single step `s1` completes successfully against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one duration event for `s1`, one attempts event with value `1` for `s1`, and one outcome event with value `SUCCESS` for `s1`

#### Scenario: failure path invokes recorder with FAILURE outcome

- **WHEN** a pipeline contains a step `s2` whose `execute` throws and the pipeline is executed against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one duration event for `s2`, one attempts event with value `1` for `s2`, and one outcome event with value `FAILURE` for `s2`

#### Scenario: recorder duration matches trace entry duration

- **WHEN** a pipeline executes against a `RecordingMetricsRecorder` and the resulting `Result` is a `Success` carrying an `ExecutionTrace`
- **THEN** for every step, the `durationNanos` recorded by the recorder MUST equal the `durationNanos` on that step's `TraceEntry`

### Requirement: Recorder exceptions are caught and do not affect the pipeline result

If any `MetricsRecorder` method throws any `Throwable`, the engine SHALL catch it, emit a WARN-level SLF4J log event with the message `metrics.recorder_failed` containing `step.id` and `error.class`/`error.message` fields, and continue execution as if the recorder had returned normally. A throwing recorder SHALL NOT cause a `Success` pipeline to become a `Failure`, SHALL NOT alter the returned `Result`'s value or trace, and SHALL NOT cause subsequent steps to be skipped on the success path.

#### Scenario: throwing recorder does not turn Success into Failure

- **WHEN** a pipeline with two steps that both complete successfully is executed against a recorder whose `recordStepDuration` throws `new RuntimeException("boom")` on every call
- **THEN** the result MUST be a `Success` whose value equals the final step's output, and a WARN log event with message `metrics.recorder_failed` MUST be captured for each step

#### Scenario: throwing recorder does not mask a genuine step failure

- **WHEN** a pipeline contains a step that throws `new IllegalStateException("real failure")` and the configured recorder also throws on every method call
- **THEN** the result MUST be a `Failure` whose `cause()` is the `IllegalStateException("real failure")` (not the recorder exception), whose `failedStepId()` equals the throwing step's id, and the recorder-failure WARN log events MUST be captured alongside the `step.error` event
