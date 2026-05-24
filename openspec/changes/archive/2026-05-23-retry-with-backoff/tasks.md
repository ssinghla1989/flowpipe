## 1. RetryPolicy Public API

- [x] 1.1 Create `RetryPolicy` final class in `io.flowpipe.api` with fields `maxAttempts`, `initialDelayMs`, `multiplier`, `jitter` and validate each in constructors
- [x] 1.2 Add `RetryPolicy.none()` static factory (maxAttempts=1, initialDelayMs=0, multiplier=1.0, jitter=false)
- [x] 1.3 Add `RetryPolicy.fixed(int maxAttempts, long delayMs)` static factory
- [x] 1.4 Add `RetryPolicy.exponential(int maxAttempts, long initialDelayMs, double multiplier, boolean jitter)` static factory

## 2. StepDescriptor Integration

- [x] 2.1 Add `RetryPolicy retryPolicy()` accessor to `StepDescriptor<I, O>`
- [x] 2.2 Add `withRetry(RetryPolicy policy)` method to `StepDescriptor` returning a new descriptor with the policy set; default to `RetryPolicy.none()`; throw `NullPointerException` for null
- [x] 2.3 Verify existing `StepDescriptor` construction paths default `retryPolicy()` to `RetryPolicy.none()` without any change to step author code

## 3. Engine Retry Loop

- [x] 3.1 Update `StepNode` to read `descriptor.retryPolicy()` and wrap the validate→execute→validate sequence in an attempt loop
- [x] 3.2 Implement delay computation: `floor(initialDelayMs * multiplier^(attempt-2))` for attempts ≥ 2; apply jitter as `uniform(0, computedDelay)` when `jitter=true`
- [x] 3.3 Implement `Thread.sleep` between failed non-final attempts; ensure no sleep before attempt 1 or after the final failed attempt
- [x] 3.4 On ultimate failure (all attempts exhausted) surface a `Failure` with the last thrown `Throwable` as `cause()` and the step id as `failedStepId()`
- [x] 3.5 Verify downstream steps are not invoked after a step exhausts all retry attempts

## 4. Observability — Retry Events

- [x] 4.1 Add `recordRetryAttempt(String stepId, int attemptNumber)` to the `MetricsRecorder` SPI
- [x] 4.2 Add a no-op implementation of `recordRetryAttempt` to `NoOpMetricsRecorder`
- [x] 4.3 Emit `step.retry` WARN-level SLF4J event with fields `step.id`, `step.attempt`, `step.max_attempts`, `step.delay_ms`, and all `RequestContext` fields after each failed non-final attempt
- [x] 4.4 Call `recorder.recordRetryAttempt(stepId, attemptNumber)` at the same point as the `step.retry` log event
- [x] 4.5 Update the final `recordStepAttempts(stepId, N)` call to pass the actual attempt count (not always 1)

## 5. Test Utilities (flowpipe-test)

- [x] 5.1 Add `recordRetryAttempt` capture to `RecordingMetricsRecorder` with accessor for assertions (e.g., `retryAttempts()` returning a list of recorded `(stepId, attemptNumber)` pairs)
- [x] 5.2 Expose a retry-count convenience assertion in `StepHarness` if applicable (e.g., `assertRetryCount(stepId, expectedCount)`)

## 6. Tests

- [x] 6.1 Test `RetryPolicy` factory validation: `maxAttempts < 1`, `initialDelayMs < 0`, `multiplier < 1.0` all throw `IllegalArgumentException`
- [x] 6.2 Test step succeeds on second attempt: `execute` called exactly twice, result is `Success`
- [x] 6.3 Test step exhausts all attempts: `execute` called exactly N times, result is `Failure` with correct `failedStepId`
- [x] 6.4 Test `RetryPolicy.none()` produces no retry and no sleep on failure
- [x] 6.5 Test input validation runs on every attempt (validator invoked N times for N attempts)
- [x] 6.6 Test output validation failure triggers retry (step called twice when first output fails validation, second passes)
- [x] 6.7 Test downstream steps are not invoked after retry exhaustion
- [x] 6.8 Test `step.retry` log event is emitted for each non-final failed attempt with correct fields
- [x] 6.9 Test no `step.retry` event emitted when `RetryPolicy.none()`
- [x] 6.10 Test `recordRetryAttempt` is called once per non-final failed attempt with correct `attemptNumber`
- [x] 6.11 Test `recordStepAttempts` receives actual attempt count (2 on first-attempt-fail-second-success; 3 on all-fail-with-maxAttempts=3)
- [x] 6.12 Test `recordStepAttempts` still receives `1` for steps that succeed on first attempt (backwards-compatible)
- [x] 6.13 Test exponential delay computation for a 4-attempt policy (delays ≈ 100, 200, 400 ms)
