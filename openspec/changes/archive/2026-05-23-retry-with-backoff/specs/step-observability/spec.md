## ADDED Requirements

### Requirement: Engine emits a step.retry log event for each retry attempt
For each failed attempt that is followed by another attempt (i.e., not the final attempt), the engine SHALL emit one SLF4J log event at `WARN` level with the message `step.retry` and at minimum these structured key-value fields: `step.id` (the step's descriptor id), `step.attempt` (the 1-indexed number of the attempt that just failed), `step.max_attempts` (the policy's `maxAttempts()`), and `step.delay_ms` (the computed sleep duration in milliseconds before the next attempt). The engine SHALL also include every `RequestContext` entry as a structured key-value pair as described in the existing start-event requirement. The `step.retry` event SHALL be emitted after the failed attempt's `step.error` event and before the sleep begins.

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

## MODIFIED Requirements

### Requirement: Engine invokes MetricsRecorder around every step on both success and failure paths
For each step invocation, after the `TraceEntry` is appended, the engine SHALL invoke the configured recorder's `recordStepDuration(stepId, durationNanos)`, `recordStepAttempts(stepId, actualAttempts)`, and `recordStepOutcome(stepId, outcome)` exactly once, where `actualAttempts` is the total number of `execute` invocations made (including the successful one if the step ultimately succeeded), `outcome` is `SUCCESS` on the success path and `FAILURE` on any error path (after all retries are exhausted). The `durationNanos` value SHALL equal the `durationNanos` field on the corresponding `TraceEntry` (total wall-clock time across all attempts including sleep). This replaces the prior requirement that `recordStepAttempts` always passed `1`.

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
