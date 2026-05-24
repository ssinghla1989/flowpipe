## 1. Public API — TimeoutPolicy and StepTimeoutException

- [x] 1.1 Create `TimeoutPolicy` final class in `io.flowpipe.api` with `long timeoutMs` field, `none()` / `ofMillis(long)` / `of(long, TimeUnit)` factory methods, and argument validation (`ms >= 1`, `duration >= 1`, non-null `unit`)
- [x] 1.2 Create `StepTimeoutException` class in `io.flowpipe.api` extending `RuntimeException` with `String stepId()` and `long timeoutMs()` accessors and a message that includes both values

## 2. StepDescriptor integration

- [x] 2.1 Add `TimeoutPolicy timeoutPolicy` component to the `StepDescriptor` record; update the compact constructor to require non-null and update all existing internal usages that construct the record directly
- [x] 2.2 Add `withTimeout(TimeoutPolicy policy)` method to `StepDescriptor` (throws `NullPointerException` on null) returning a new descriptor with the updated policy
- [x] 2.3 Add `withTimeout(TimeoutPolicy policy)` method to `StepDescriptor.Builder` and wire it into `build()`; default is `TimeoutPolicy.none()`

## 3. Engine enforcement

- [x] 3.1 Update `Pipeline.executeItemWithRetry` to detect `timeoutPolicy().timeoutMs() > 0`; when set, submit the `invokeStep` call to `executor` via a `Callable` and use `future.get(timeoutMs, MILLISECONDS)` to enforce the deadline
- [x] 3.2 On `TimeoutException` from `future.get(...)`, call `future.cancel(true)`, wrap in a `StepTimeoutException(stepId, timeoutMs)`, and let the retry loop treat it as a regular failure (same as any other `Throwable`)
- [x] 3.3 Ensure steps with `TimeoutPolicy.none()` continue to run inline (no executor submission, no behaviour change)

## 4. Tests — TimeoutPolicy value type

- [x] 4.1 Test `TimeoutPolicy.none()` has `timeoutMs == 0`
- [x] 4.2 Test `TimeoutPolicy.ofMillis(500)` has `timeoutMs == 500`
- [x] 4.3 Test `TimeoutPolicy.of(2, SECONDS)` has `timeoutMs == 2000`
- [x] 4.4 Test `ofMillis(0)` and `ofMillis(-1)` throw `IllegalArgumentException`
- [x] 4.5 Test `of(0, SECONDS)` throws `IllegalArgumentException` and `of(1, null)` throws `NullPointerException`

## 5. Tests — StepDescriptor wiring

- [x] 5.1 Test default `timeoutPolicy()` on a freshly built descriptor is `TimeoutPolicy.none()`
- [x] 5.2 Test `descriptor.withTimeout(TimeoutPolicy.ofMillis(300))` returns a descriptor with `timeoutMs == 300`
- [x] 5.3 Test `descriptor.withTimeout(null)` throws `NullPointerException`
- [x] 5.4 Test `Builder.withTimeout(TimeoutPolicy.ofMillis(100)).build()` yields `timeoutMs == 100`

## 6. Tests — Engine timeout enforcement

- [x] 6.1 Test a step with `TimeoutPolicy.ofMillis(500)` that completes in ~10ms produces `Success` with correct output
- [x] 6.2 Test a step with `TimeoutPolicy.ofMillis(50)` that sleeps for 500ms produces `Failure` whose `cause()` is `StepTimeoutException` with the correct `stepId` and `timeoutMs`
- [x] 6.3 Test downstream step is not invoked after a timeout failure
- [x] 6.4 Test `StepTimeoutException.getMessage()` contains the step id and timeout value

## 7. Tests — Timeout and retry interaction

- [x] 7.1 Test a step with `RetryPolicy.fixed(2, 0)` + `TimeoutPolicy.ofMillis(50)` that times out on the first attempt but completes within the deadline on the second attempt produces `Success` with `execute` called exactly twice
- [x] 7.2 Test a step with `RetryPolicy.fixed(3, 0)` + `TimeoutPolicy.ofMillis(50)` that times out on every attempt produces `Failure` with `cause()` being `StepTimeoutException` and `execute` called exactly 3 times
