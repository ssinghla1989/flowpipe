## 1. Dependency Confirmation

- [x] 1.1 Verify `dev.failsafe:failsafe:3.3.2` is declared as `implementation` in `flowpipe-core/build.gradle.kts` and the project compiles clean (`./gradlew :flowpipe-core:compileJava`)
- [x] 1.2 Confirm Failsafe 3.3.2 Javadoc for `CircuitBreaker.builder()` method signatures: verify that `withFailureRateThreshold`, `withSlidingWindow`, and `withSuccessThreshold` exist and accept the parameter types expected by the design's mapping (document exact signatures in a comment at the top of the translation method)

## 2. Policy Translation Factory

- [x] 2.1 Create a package-private final class `FailsafePolicies` in `io.flowpipe.engine` with three static factory methods: `toFailsafe(RetryPolicy, String stepId)`, `toFailsafe(TimeoutPolicy, String stepId)`, and `toFailsafe(CircuitBreakerPolicy, String stepId)`
- [x] 2.2 Implement `toFailsafe(RetryPolicy, ...)`: map `maxAttempts` → `withMaxAttempts`, `initialDelayMs + multiplier` → `withBackoff`, `jitter` → `withJitter`; use `RetryPolicy.none()` / `maxAttempts == 1` check to skip retry policy wrapping when no retry is configured
- [x] 2.3 Implement `toFailsafe(TimeoutPolicy, ...)`: return `dev.failsafe.Timeout.of(Duration.ofMillis(timeoutMs)).withInterrupt(true)`; return `null` (or skip) when `timeoutMs == 0` to avoid wrapping steps with no timeout
- [x] 2.4 Implement `toFailsafe(CircuitBreakerPolicy, ...)`: map all five fields onto the Failsafe `CircuitBreaker.builder()` API; confirm that the `halfOpenProbeCount` → `withSuccessThreshold` mapping enforces the correct close-on-N-probes behavior

## 3. Replace Custom Retry Loop

- [x] 3.1 In `Pipeline.executeItemWithRetry`, replace the manual `for (int attempt = 1; attempt <= maxAttempts; attempt++)` loop with a Failsafe execution using the `dev.failsafe.RetryPolicy<Object>` from `FailsafePolicies.toFailsafe(retryPolicy)`
- [x] 3.2 Preserve SLF4J `step.start` / `step.retry` / `step.finish` / `step.error` log emission and `MetricsRecorder.recordRetryAttempt` calls; wire these as Failsafe event listeners (`onRetry`, `onFailedAttempt`) on the Failsafe `RetryPolicy` object
- [x] 3.3 After the Failsafe call, unwrap any `dev.failsafe.FailsafeException` by calling `.getCause()` before constructing the `Failure` result; ensure the original step-thrown `Throwable` surfaces in `Failure.cause()`
- [x] 3.4 Delete the private `computeDelay(RetryPolicy, int)` method and `sleepMillis(long)` helper from `Pipeline` (now handled internally by Failsafe)

## 4. Replace Custom Timeout Mechanism

- [x] 4.1 In `Pipeline.executeItemWithRetry`, remove the `invokeStepWithTimeout(step, input, ctx, stepId, timeoutMs)` call and replace with Failsafe `Timeout<Object>` from `FailsafePolicies.toFailsafe(timeoutPolicy)` composed into the Failsafe execution
- [x] 4.2 Catch `dev.failsafe.TimeoutExceededException` in the execution error handler and translate it to `new StepTimeoutException(stepId, timeoutPolicy.timeoutMs())` before constructing `Failure`
- [x] 4.3 Delete the private `invokeStepWithTimeout(...)` method from `Pipeline`
- [x] 4.4 Remove the now-unnecessary `java.util.concurrent.Future`, `TimeoutException`, and `ExecutionException` imports from `Pipeline.java`

## 5. Replace Custom Circuit Breaker

- [x] 5.1 In `PipelineBuilder.build()` (or `Pipeline` constructor), replace the construction of `CircuitBreakerRegistry` with a `Map<String, dev.failsafe.CircuitBreaker<Object>>` built eagerly for each `StepNode` that has a non-null `CircuitBreakerPolicy`; use `FailsafePolicies.toFailsafe(circuitBreakerPolicy, stepId)` to build each Failsafe `CircuitBreaker` instance
- [x] 5.2 In `Pipeline.executeItemWithRetry`, compose the Failsafe `CircuitBreaker` into the Failsafe execution (outermost policy) when one exists for the step id
- [x] 5.3 Catch `dev.failsafe.CircuitBreakerOpenException` in the execution error handler and translate it to `new io.flowpipe.api.CircuitBreakerOpenException(stepId, retriableAfter)` where `retriableAfter` is derived from the Failsafe exception or computed from `openWindowMs`
- [x] 5.4 Remove the `circuitBreakerRegistry` field, the `CircuitBreakerRegistry` constructor parameter, and all `circuitBreaker.allowCall()` / `circuitBreaker.recordSuccess()` / `circuitBreaker.recordFailure()` call sites from `Pipeline`
- [x] 5.5 Delete `CircuitBreaker.java` from `io.flowpipe.engine`
- [x] 5.6 Delete `CircuitBreakerRegistry.java` from `io.flowpipe.engine`
- [x] 5.7 Remove the `CircuitBreakerRegistry` construction and field from `PipelineBuilder`

## 6. Policy Composition Wiring

- [x] 6.1 Assemble the Failsafe policy list in `executeItemWithRetry` in outer-to-inner order — `[circuitBreaker (if present), retryPolicy (if maxAttempts > 1), timeout (if timeoutMs > 0)]` — and call `Failsafe.with(policies).get(callable)` where `callable` is `() -> invokeStep(step, input, ctx)`
- [x] 6.2 Verify that when only `RetryPolicy.none()` is configured (the default), no Failsafe retry policy is composed and the step is called exactly once with no retry overhead

## 7. Observability Preservation

- [x] 7.1 Confirm that `step.start`, `step.finish`, `step.error`, and `step.retry` SLF4J structured log events still fire correctly for steps exercising retry, timeout, and circuit-open paths
- [x] 7.2 Confirm that `MetricsRecorder.recordRetryAttempt` is called for each retry attempt and `StepOutcome.SUCCESS` / `StepOutcome.FAILURE` are emitted correctly via Failsafe event listeners

## 8. Test Verification

- [x] 8.1 Run `./gradlew :flowpipe-core:test --tests io.flowpipe.engine.RetryTest` and confirm all tests pass
- [x] 8.2 Run `./gradlew :flowpipe-core:test --tests io.flowpipe.engine.StepTimeoutExecutionTest` and confirm all tests pass
- [x] 8.3 Run `./gradlew :flowpipe-core:test --tests io.flowpipe.engine.TimeoutPolicyTest` and confirm all tests pass
- [x] 8.4 Run `./gradlew :flowpipe-core:test --tests io.flowpipe.engine.CircuitBreakerExecutionTest` and confirm all tests pass
- [x] 8.5 Run `./gradlew :flowpipe-core:test --tests io.flowpipe.engine.CircuitBreakerPolicyTest` and confirm all tests pass
- [x] 8.6 Run the full test suite `./gradlew build` and confirm zero failures and zero compiler warnings
