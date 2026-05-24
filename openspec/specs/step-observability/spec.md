# step-observability

## Purpose

Defines the structured SLF4J log events emitted automatically by the engine around every step invocation (start, finish, and error), and the `MetricsRecorder` SPI through which duration, attempt count, and outcome signals are delivered to any metrics backend — all without requiring step authors to write any logging or metrics code.

## Requirements

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

The library SHALL define a `MetricsRecorder` interface in `io.flowpipe.observability` with exactly four methods: `recordStepDuration(String stepId, long durationNanos)`, `recordStepAttempts(String stepId, int attempts)`, `recordStepOutcome(String stepId, StepOutcome outcome)`, and `recordRetryAttempt(String stepId, int attemptNumber)`, where `StepOutcome` is an enum with values `SUCCESS`, `FAILURE`, and `SKIPPED`. The library SHALL ship `NoOpMetricsRecorder` as a stateless singleton accessible via a static `instance()` accessor whose methods do nothing.

#### Scenario: NoOpMetricsRecorder methods are no-ops

- **WHEN** any of `NoOpMetricsRecorder.instance().recordStepDuration("x", 1L)`, `recordStepAttempts("x", 1)`, `recordStepOutcome("x", StepOutcome.SUCCESS)`, or `recordRetryAttempt("x", 1)` is invoked
- **THEN** the call MUST return normally without throwing and MUST have no observable side effect

#### Scenario: NoOpMetricsRecorder handles SKIPPED outcome without throwing

- **WHEN** `NoOpMetricsRecorder.instance().recordStepOutcome("x", StepOutcome.SKIPPED)` is invoked
- **THEN** the call MUST return normally without throwing

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

For each step invocation, after the `TraceEntry` is appended, the engine SHALL invoke the configured recorder's `recordStepDuration(stepId, durationNanos)`, `recordStepAttempts(stepId, actualAttempts)`, and `recordStepOutcome(stepId, outcome)` exactly once, where `actualAttempts` is the total number of `execute` invocations made (including the successful one if the step ultimately succeeded), `outcome` is `SUCCESS` on the success path and `FAILURE` on any error path (after all retries are exhausted). The `durationNanos` value SHALL equal the `durationNanos` field on the corresponding `TraceEntry` (total wall-clock time across all attempts including sleep).

#### Scenario: success path with no retry invokes recorder with attempts=1 and SUCCESS outcome

- **WHEN** a pipeline with a single step `s1` using `RetryPolicy.none()` completes successfully against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one duration event for `s1`, one attempts event with value `1` for `s1`, and one outcome event with value `SUCCESS` for `s1`

#### Scenario: success on second attempt invokes recorder with attempts=2 and SUCCESS outcome

- **WHEN** a step with id `"flaky"` and `RetryPolicy.fixed(3, 0)` fails on attempt 1 and succeeds on attempt 2, and the pipeline runs against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one attempts event for `"flaky"` with value `2`, and one outcome event with value `SUCCESS`

#### Scenario: failure after all retries invokes recorder with correct attempt count and FAILURE outcome

- **WHEN** a step with id `"broken"` and `RetryPolicy.fixed(3, 0)` fails on all three attempts against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly one attempts event for `"broken"` with value `3`, and one outcome event with value `FAILURE`

#### Scenario: recorder duration matches trace entry duration across all attempts

- **WHEN** a pipeline with a step that retries once executes against a `RecordingMetricsRecorder` and the resulting `Result` is a `Success` carrying an `ExecutionTrace`
- **THEN** the `durationNanos` recorded by the recorder MUST equal the `durationNanos` on that step's `TraceEntry`

### Requirement: Recorder exceptions are caught and do not affect the pipeline result

If any `MetricsRecorder` method throws any `Throwable`, the engine SHALL catch it, emit a WARN-level SLF4J log event with the message `metrics.recorder_failed` containing `step.id` and `error.class`/`error.message` fields, and continue execution as if the recorder had returned normally. A throwing recorder SHALL NOT cause a `Success` pipeline to become a `Failure`, SHALL NOT alter the returned `Result`'s value or trace, and SHALL NOT cause subsequent steps to be skipped on the success path.

#### Scenario: throwing recorder does not turn Success into Failure

- **WHEN** a pipeline with two steps that both complete successfully is executed against a recorder whose `recordStepDuration` throws `new RuntimeException("boom")` on every call
- **THEN** the result MUST be a `Success` whose value equals the final step's output, and a WARN log event with message `metrics.recorder_failed` MUST be captured for each step

#### Scenario: throwing recorder does not mask a genuine step failure

- **WHEN** a pipeline contains a step that throws `new IllegalStateException("real failure")` and the configured recorder also throws on every method call
- **THEN** the result MUST be a `Failure` whose `cause()` is the `IllegalStateException("real failure")` (not the recorder exception), whose `failedStepId()` equals the throwing step's id, and the recorder-failure WARN log events MUST be captured alongside the `step.error` event

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

### Requirement: Engine emits a step.retry log event for each retry attempt

For each failed attempt that is followed by another attempt (i.e., not the final attempt), the engine SHALL emit one SLF4J log event at `WARN` level with the message `step.retry` and at minimum these structured key-value fields: `step.id` (the step's descriptor id), `step.attempt` (the 1-indexed number of the attempt that just failed), `step.max_attempts` (the policy's `maxAttempts()`), and `step.delay_ms` (the computed sleep duration in milliseconds before the next attempt). The engine SHALL also include every `RequestContext` entry as a structured key-value pair as described in the start-event requirement. The `step.retry` event SHALL be emitted after the failed attempt's `step.error` event and before the sleep begins.

#### Scenario: step.retry log is emitted after each non-final failed attempt

- **WHEN** a step with id `"flaky"` and `RetryPolicy.fixed(3, 100)` fails on the first two attempts and succeeds on the third
- **THEN** the captured log events MUST contain exactly two events with message `step.retry` and level `WARN`, one with `step.id="flaky"` and `step.attempt=1`, another with `step.id="flaky"` and `step.attempt=2`, each carrying `step.max_attempts=3`

#### Scenario: No step.retry event is emitted on final failed attempt

- **WHEN** a step with `RetryPolicy.fixed(2, 0)` fails on both attempts
- **THEN** exactly one `step.retry` event MUST be emitted (for the first failure) and no `step.retry` event MUST be emitted after the second (final) failure

#### Scenario: No step.retry event is emitted when RetryPolicy.none() is configured

- **WHEN** a step with `RetryPolicy.none()` fails
- **THEN** no `step.retry` log event MUST be emitted

### Requirement: MetricsRecorder SPI exposes a per-attempt retry hook

The `MetricsRecorder` interface SHALL define an additional method `recordRetryAttempt(String stepId, int attemptNumber)`. `NoOpMetricsRecorder` SHALL implement this method as a no-op. `RecordingMetricsRecorder` in `flowpipe-test` SHALL record each call with its arguments for assertion. The engine SHALL invoke `recordRetryAttempt(stepId, attemptNumber)` once for each failed non-final attempt, where `attemptNumber` is the 1-indexed number of the attempt that just failed, at the same time it emits the `step.retry` log event.

#### Scenario: recordRetryAttempt is called once per non-final failed attempt

- **WHEN** a step with id `"unstable"` and `RetryPolicy.fixed(3, 0)` fails on the first two attempts and succeeds on the third, and the pipeline runs against a `RecordingMetricsRecorder`
- **THEN** the recorder MUST observe exactly two `recordRetryAttempt` events for `"unstable"`, one with `attemptNumber=1` and one with `attemptNumber=2`

#### Scenario: recordRetryAttempt is not called on final attempt failure

- **WHEN** a step with `RetryPolicy.fixed(2, 0)` fails on both attempts against a `RecordingMetricsRecorder`
- **THEN** exactly one `recordRetryAttempt` event MUST be observed for the step (for attempt 1), and none for the final failure

#### Scenario: recordRetryAttempt is not called when no retry occurs

- **WHEN** a step with `RetryPolicy.none()` fails
- **THEN** `recordRetryAttempt` MUST NOT be called
