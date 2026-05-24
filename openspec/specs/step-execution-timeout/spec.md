# step-execution-timeout

## Purpose

Defines per-step execution timeouts — how a `TimeoutPolicy` is declared on a `StepDescriptor`, how the engine enforces the deadline around each step invocation, the `StepTimeoutException` surfaced on breach, and how timeouts compose with `RetryPolicy` so that each retry attempt receives its own independent deadline.

## Requirements

### Requirement: TimeoutPolicy is an immutable value type with factory methods

The library SHALL define `TimeoutPolicy` as a public final class in `io.flowpipe.api` with one field: `long timeoutMs` (the deadline in milliseconds; `0` means no timeout). The class SHALL provide the following static factory methods: `TimeoutPolicy.none()` (timeoutMs=0, meaning no deadline enforced), `TimeoutPolicy.ofMillis(long ms)` (explicit millisecond deadline; `ms >= 1` required), and `TimeoutPolicy.of(long duration, TimeUnit unit)` (converts to milliseconds; `duration >= 1` and unit non-null required). Passing `ms < 1` to `ofMillis` SHALL throw `IllegalArgumentException`. Passing `duration < 1` or `null` unit to `of` SHALL throw `IllegalArgumentException` or `NullPointerException` respectively.

#### Scenario: TimeoutPolicy.none() has timeoutMs equal to zero

- **WHEN** `TimeoutPolicy.none()` is called
- **THEN** the returned policy MUST have `timeoutMs()` equal to `0`

#### Scenario: TimeoutPolicy.ofMillis produces policy with correct deadline

- **WHEN** `TimeoutPolicy.ofMillis(500)` is called
- **THEN** the returned policy MUST have `timeoutMs()` equal to `500`

#### Scenario: TimeoutPolicy.of converts duration and unit to milliseconds

- **WHEN** `TimeoutPolicy.of(2, TimeUnit.SECONDS)` is called
- **THEN** the returned policy MUST have `timeoutMs()` equal to `2000`

#### Scenario: ofMillis with zero or negative duration throws IllegalArgumentException

- **WHEN** `TimeoutPolicy.ofMillis(0)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: of with zero duration throws IllegalArgumentException

- **WHEN** `TimeoutPolicy.of(0, TimeUnit.SECONDS)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: of with null unit throws NullPointerException

- **WHEN** `TimeoutPolicy.of(1, null)` is called
- **THEN** a `NullPointerException` MUST be thrown

### Requirement: StepDescriptor carries a TimeoutPolicy accessible to the engine

`StepDescriptor<I, O>` SHALL expose a `TimeoutPolicy timeoutPolicy()` accessor. `StepDescriptor` SHALL provide a `withTimeout(TimeoutPolicy policy)` method that returns a new `StepDescriptor` with the specified policy set. If `withTimeout(null)` is called it SHALL throw `NullPointerException`. When `withTimeout(...)` is not called, `timeoutPolicy()` SHALL return `TimeoutPolicy.none()`. The `StepDescriptor.Builder` SHALL expose a `withTimeout(TimeoutPolicy)` method with the same semantics.

#### Scenario: Default timeout policy is none

- **WHEN** a `StepDescriptor` is constructed via `Builder` without calling `withTimeout(...)`
- **THEN** `descriptor.timeoutPolicy().timeoutMs()` MUST equal `0`

#### Scenario: withTimeout sets the policy on the descriptor

- **WHEN** `descriptor.withTimeout(TimeoutPolicy.ofMillis(300))` is called on an existing descriptor
- **THEN** the returned descriptor's `timeoutPolicy().timeoutMs()` MUST equal `300`

#### Scenario: withTimeout(null) throws NullPointerException

- **WHEN** `descriptor.withTimeout(null)` is called
- **THEN** a `NullPointerException` MUST be thrown

#### Scenario: Builder.withTimeout sets the policy

- **WHEN** `StepDescriptor.builder(...).withTimeout(TimeoutPolicy.ofMillis(100)).build()` is called
- **THEN** the resulting descriptor's `timeoutPolicy().timeoutMs()` MUST equal `100`

### Requirement: Engine enforces the per-step timeout around each execute invocation

For every step invocation where `descriptor.timeoutPolicy().timeoutMs() > 0`, the engine SHALL enforce that `step.execute(...)` (including input validation, the execute call, and output validation) completes within `timeoutMs` milliseconds. If the deadline is exceeded, the engine SHALL interrupt the executing thread, and the step invocation SHALL be treated as a failure whose cause is a `StepTimeoutException`. Steps with `TimeoutPolicy.none()` (timeoutMs=0) SHALL run without any time bound, preserving existing behaviour.

#### Scenario: Step completing within its timeout succeeds normally

- **WHEN** a step with `TimeoutPolicy.ofMillis(500)` completes in 10 milliseconds
- **THEN** the pipeline result MUST be a `Success`, and the step output MUST equal the step's return value

#### Scenario: Step exceeding its timeout produces a Failure with StepTimeoutException

- **WHEN** a step with `TimeoutPolicy.ofMillis(50)` blocks for longer than 50 milliseconds
- **THEN** the pipeline result MUST be a `Failure`, `Failure.cause()` MUST be an instance of `StepTimeoutException`, and `Failure.failedStepId()` MUST equal the timed-out step's id

#### Scenario: Downstream steps are not invoked after a timeout failure

- **WHEN** a pipeline has two sequential steps and the first step times out
- **THEN** the second step's `execute` MUST NOT be called, and the pipeline result MUST be a `Failure`

#### Scenario: Step without a timeout runs without any time bound

- **WHEN** a step with `TimeoutPolicy.none()` (the default) takes longer than any fixed duration
- **THEN** the engine MUST NOT interrupt it on account of a timeout; the step runs to completion

### Requirement: StepTimeoutException carries the step id and configured deadline

`StepTimeoutException` SHALL be a public class in `io.flowpipe.api` that extends `RuntimeException`. It SHALL carry a `String stepId()` accessor returning the id of the timed-out step, and a `long timeoutMs()` accessor returning the configured deadline in milliseconds. Its `getMessage()` SHALL include both the step id and the timeout value in a human-readable form.

#### Scenario: StepTimeoutException exposes step id and timeout

- **WHEN** a step with id `"fetch-user"` and `TimeoutPolicy.ofMillis(100)` times out
- **THEN** the `StepTimeoutException` surfaced via `Failure.cause()` MUST have `stepId()` equal to `"fetch-user"` and `timeoutMs()` equal to `100`

#### Scenario: StepTimeoutException message includes step id and deadline

- **WHEN** a `StepTimeoutException` is constructed for step `"fetch-user"` with timeout 100ms
- **THEN** `exception.getMessage()` MUST contain both `"fetch-user"` and `"100"`

### Requirement: Timeout interacts with RetryPolicy — each attempt receives its own independent deadline

When a step has both a `TimeoutPolicy` with `timeoutMs > 0` and a `RetryPolicy` with `maxAttempts > 1`, the engine SHALL give each attempt its own independent deadline. A timeout on one attempt SHALL be treated as a failure for that attempt; if further attempts remain under the `RetryPolicy`, the engine SHALL proceed with the next attempt with a fresh deadline. The retry delay (if any) SHALL occur between attempts and is not counted against the next attempt's deadline.

#### Scenario: Step timing out on first attempt is retried with a fresh deadline

- **WHEN** a step has `RetryPolicy.fixed(2, 0)` and `TimeoutPolicy.ofMillis(50)`, blocks past the deadline on the first attempt, but completes within 50 milliseconds on the second attempt
- **THEN** the pipeline result MUST be a `Success`, and `execute` MUST have been called exactly twice

#### Scenario: Step timing out on every attempt exhausts the retry policy

- **WHEN** a step has `RetryPolicy.fixed(3, 0)` and `TimeoutPolicy.ofMillis(50)`, and blocks past the deadline on all three attempts
- **THEN** the pipeline result MUST be a `Failure` whose `cause()` is a `StepTimeoutException`, and `execute` MUST have been called exactly `3` times

#### Scenario: Timeout is invisible to step authors alongside retry

- **WHEN** a developer implements `Step<I, O>` with both a `RetryPolicy` and a `TimeoutPolicy` configured on its `StepDescriptor`
- **THEN** the compiler MUST NOT require any timeout-related or retry-related method beyond `describe()` and `execute(I, StepContext)`
