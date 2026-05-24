## ADDED Requirements

### Requirement: RetryPolicy is an immutable value type with four fields
The library SHALL define `RetryPolicy` as a public final class in `io.flowpipe.api` with four fields: `int maxAttempts` (minimum 1), `long initialDelayMs` (minimum 0), `double multiplier` (minimum 1.0), and `boolean jitter`. The class SHALL provide the following static factory methods: `RetryPolicy.none()` (maxAttempts=1, initialDelayMs=0, multiplier=1.0, jitter=false), `RetryPolicy.fixed(int maxAttempts, long delayMs)` (constant-delay retry), and `RetryPolicy.exponential(int maxAttempts, long initialDelayMs, double multiplier, boolean jitter)`. Passing `maxAttempts < 1` to any factory SHALL throw `IllegalArgumentException`. Passing `initialDelayMs < 0` SHALL throw `IllegalArgumentException`. Passing `multiplier < 1.0` SHALL throw `IllegalArgumentException`.

#### Scenario: RetryPolicy.none() has maxAttempts=1 and zero delay
- **WHEN** `RetryPolicy.none()` is called
- **THEN** the returned policy MUST have `maxAttempts()` equal to `1`, `initialDelayMs()` equal to `0`, `multiplier()` equal to `1.0`, and `jitter()` equal to `false`

#### Scenario: RetryPolicy.fixed produces constant-delay policy
- **WHEN** `RetryPolicy.fixed(3, 500)` is called
- **THEN** the returned policy MUST have `maxAttempts()` equal to `3` and `initialDelayMs()` equal to `500`

#### Scenario: Invalid maxAttempts throws IllegalArgumentException
- **WHEN** `RetryPolicy.fixed(0, 100)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: Negative initialDelayMs throws IllegalArgumentException
- **WHEN** `RetryPolicy.fixed(2, -1)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

### Requirement: StepDescriptor carries a RetryPolicy accessible to the engine
`StepDescriptor<I, O>` SHALL expose a `RetryPolicy retryPolicy()` accessor. `StepDescriptor` SHALL provide a `withRetry(RetryPolicy policy)` method that returns a new `StepDescriptor` with the specified policy set. If `withRetry(null)` is called it SHALL throw `NullPointerException`. When `withRetry(...)` is not called, `retryPolicy()` SHALL return `RetryPolicy.none()`.

#### Scenario: Default retry policy is none
- **WHEN** a `StepDescriptor` is constructed without calling `withRetry(...)`
- **THEN** `descriptor.retryPolicy()` MUST return a policy whose `maxAttempts()` is `1`

#### Scenario: withRetry sets the policy
- **WHEN** `descriptor.withRetry(RetryPolicy.fixed(3, 200))` is called
- **THEN** the returned descriptor's `retryPolicy().maxAttempts()` MUST equal `3`

#### Scenario: withRetry(null) throws NullPointerException
- **WHEN** `descriptor.withRetry(null)` is called
- **THEN** a `NullPointerException` MUST be thrown

### Requirement: Engine retries a failed step according to its RetryPolicy
For each step invocation, the engine SHALL attempt `step.execute(...)` up to `retryPolicy().maxAttempts()` times. On each attempt that throws a `Throwable`, if another attempt remains the engine SHALL sleep for the computed delay and invoke `step.execute(...)` again. If all attempts are exhausted the engine SHALL surface a `Failure` as if no retry occurred. Input and output validation SHALL run fresh on each attempt. The computed sleep delay for attempt number `n` (1-indexed, `n >= 2`) SHALL be `floor(initialDelayMs * multiplier^(n-2))` milliseconds; when `jitter` is `true`, the actual sleep SHALL be `uniform(0, computedDelay)`. No sleep SHALL occur before the first attempt or after the final failed attempt.

#### Scenario: Step succeeds on second attempt after transient failure
- **WHEN** a step with `RetryPolicy.fixed(3, 0)` throws on the first `execute` call and succeeds on the second
- **THEN** the pipeline result MUST be a `Success` whose value equals the second call's return value, and `execute` MUST have been called exactly twice

#### Scenario: Step exhausts all attempts and pipeline returns Failure
- **WHEN** a step with `RetryPolicy.fixed(3, 0)` throws on every `execute` call
- **THEN** the pipeline result MUST be a `Failure`, and `execute` MUST have been called exactly `3` times

#### Scenario: RetryPolicy.none() means no retry on failure
- **WHEN** a step with the default `RetryPolicy.none()` throws on `execute`
- **THEN** the pipeline result MUST be a `Failure`, `execute` MUST have been called exactly `1` time, and no sleep MUST occur

#### Scenario: Input validation runs fresh on each retry attempt
- **WHEN** a step with `RetryPolicy.fixed(2, 0)` has an input validator and the first `execute` call fails
- **THEN** input validation MUST be invoked before each `execute` call (i.e., twice total for two attempts)

#### Scenario: Output validation failure on first attempt triggers retry
- **WHEN** a step with `RetryPolicy.fixed(2, 0)` whose `execute` returns a value that fails output validation on the first attempt, and whose output validation passes on the second attempt
- **THEN** the pipeline result MUST be a `Success`, and `execute` MUST have been called twice

### Requirement: Retry delay uses exponential backoff with optional jitter
The delay before attempt `n` (n >= 2) SHALL equal `floor(initialDelayMs * multiplier^(n-2))` milliseconds. When `jitter` is `true`, the actual delay SHALL be chosen uniformly at random in `[0, computedDelay]`. No delay SHALL be applied before the first attempt or after the final attempt regardless of outcome.

#### Scenario: Exponential delay doubles between attempts
- **WHEN** a step with `RetryPolicy.exponential(4, 100, 2.0, false)` fails on every attempt and sleep durations are recorded
- **THEN** the recorded sleep durations MUST be approximately `[100, 200, 400]` milliseconds (before attempts 2, 3, 4)

#### Scenario: No sleep occurs before first attempt
- **WHEN** a step with any `RetryPolicy` is invoked and the engine is observed for sleeps
- **THEN** no sleep MUST occur before the first `execute` call

#### Scenario: No sleep occurs after the final failed attempt
- **WHEN** a step with `RetryPolicy.fixed(2, 500)` fails on both attempts
- **THEN** no sleep MUST occur after the second (final) failed `execute` call

### Requirement: Retry does not affect downstream steps on ultimate failure
When a step exhausts its retry attempts and the engine records a `Failure`, all downstream steps in the pipeline SHALL NOT be invoked, consistent with the existing single-attempt failure behaviour.

#### Scenario: Downstream steps are not invoked after retry exhaustion
- **WHEN** a pipeline has two steps and the first step exhausts its `RetryPolicy.fixed(2, 0)` retries
- **THEN** the second step's `execute` MUST NOT be called, and the pipeline result MUST be a `Failure` whose `failedStepId()` equals the first step's id

### Requirement: Retry is invisible to step authors
Step authors SHALL implement only `describe()` and `execute(I, StepContext)`. The retry loop SHALL be entirely engine-managed. No method, annotation, or marker interface related to retry SHALL be required or visible on the `Step` interface.

#### Scenario: Step interface has no retry-related methods
- **WHEN** a developer implements `Step<I, O>`
- **THEN** the compiler MUST NOT require any retry-related method beyond `describe()` and `execute(I, StepContext)`
