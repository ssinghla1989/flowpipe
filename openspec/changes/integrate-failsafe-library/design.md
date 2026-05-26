## Context

FlowPipe's `Pipeline` class contains a hand-rolled resilience stack: a manual retry loop with exponential backoff, a `Future.get(timeout)` timeout mechanism, and a custom `CircuitBreaker` + `CircuitBreakerRegistry` state machine (the latter added by the in-flight `circuit-breaker-policy` change). Together these are ~250 lines of bespoke concurrency code across `Pipeline.executeItemWithRetry`, `invokeStepWithTimeout`, `CircuitBreaker`, and `CircuitBreakerRegistry`.

Failsafe (v3, `dev.failsafe:failsafe`) is a mature, zero-dependency Java library that provides production-quality `RetryPolicy`, `Timeout`, and `CircuitBreaker` as composable policies. Its composition model directly matches FlowPipe's intended execution order: circuit-check wraps retry, retry wraps timeout, timeout wraps the step. Failsafe is Java 11+ compatible (FlowPipe targets Java 17).

The public API types — `RetryPolicy`, `TimeoutPolicy`, `CircuitBreakerPolicy`, `StepTimeoutException`, `CircuitBreakerOpenException` — are not changing. Failsafe is a private implementation detail; callers never see Failsafe types.

## Goals / Non-Goals

**Goals:**
- Replace the custom retry loop, timeout mechanism, and circuit breaker state machine with Failsafe equivalents.
- Delete `CircuitBreaker.java` and `CircuitBreakerRegistry.java`.
- Preserve all behavioral requirements of `step-retry`, `step-execution-timeout`, and `circuit-breaker-policy` specs exactly.
- Keep the FlowPipe public API (`RetryPolicy`, `TimeoutPolicy`, `CircuitBreakerPolicy`, exception types) unchanged.
- Translate Failsafe's internal exceptions (`TimeoutExceededException`, `dev.failsafe.CircuitBreakerOpenException`) to FlowPipe's own at the execution boundary.

**Non-Goals:**
- Exposing any Failsafe type in the FlowPipe public API.
- Adding Failsafe-specific features that aren't already in the FlowPipe policy types (e.g., per-exception-type retry predicates, time-based sliding windows).
- Changing the `MetricsRecorder` SPI or observability emission.
- Using Failsafe's async execution; FlowPipe's timeout already uses the pipeline's `ExecutorService`.

## Decisions

### 1. Policy composition order

Failsafe policies are composed outermost-first: `Failsafe.with(cb, retry, timeout)`. With this ordering:

- `CircuitBreaker` is outermost — if OPEN, the execution short-circuits immediately with no retry and no timeout.
- `RetryPolicy` wraps `Timeout` — each retry attempt gets its own independent timeout deadline (matching the existing spec requirement).
- `Timeout` is innermost — enforces the per-attempt deadline around `step.execute`.

This maps directly to FlowPipe's current execution order and preserves all existing behavior.

When no `CircuitBreakerPolicy` is set, `Failsafe.with(retry, timeout)` is used. When no `TimeoutPolicy` is set, `Failsafe.with(cb, retry)` is used. When only retry is configured (the common case), `Failsafe.with(retry)` is used.

### 2. Translation layer: FlowPipe policy types → Failsafe policy objects

A private factory method (e.g., `FailsafePolicies.retryFor(RetryPolicy, Class<O>)`) converts FlowPipe value types to Failsafe policy objects at execution time. This keeps the translation in one place and avoids scattering Failsafe API calls through `Pipeline`.

**`RetryPolicy` → `dev.failsafe.RetryPolicy<O>`:**
- `maxAttempts` → `.withMaxAttempts(maxAttempts)`
- `initialDelayMs + multiplier + jitter` → `.withBackoff(initialDelayMs, Long.MAX_VALUE, ChronoUnit.MILLIS, multiplier)` with `.withJitter(0.5)` when jitter is true (or keep manual jitter to match current uniform-random behavior exactly — see Decision 5).

**`TimeoutPolicy` → `dev.failsafe.Timeout<O>`:**
- `timeoutMs` → `Timeout.of(Duration.ofMillis(timeoutMs))`
- Set `.withInterrupt(true)` so Failsafe cancels/interrupts the step thread on deadline breach.

**`CircuitBreakerPolicy` → `dev.failsafe.CircuitBreaker<O>`:**
- `failureRateThreshold + minimumCalls + slidingWindowSize + openWindowMs` → `CircuitBreaker.builder().withFailureRateThreshold(failureRateThreshold, minimumCalls, Duration.ofMillis(openWindowMs)).withSlidingWindow(slidingWindowSize)` (exact method names subject to Failsafe 3.x builder API; verify during implementation).
- `halfOpenProbeCount` → `.withSuccessThreshold(halfOpenProbeCount)`.

### 3. Exception translation at the FlowPipe boundary

Failsafe wraps results in its own exception types. After `Failsafe.with(...).get(...)` throws:

- `dev.failsafe.TimeoutExceededException` → `new StepTimeoutException(stepId, timeoutMs)`. Failsafe's exception does not carry step metadata; we have that from context.
- `dev.failsafe.CircuitBreakerOpenException` → `new io.flowpipe.api.CircuitBreakerOpenException(stepId, retriableAfter)`. Failsafe's exception carries the `CircuitBreaker` reference; we extract the estimated retry-ready time from it.
- All other throwables are re-thrown as-is (they are step-thrown exceptions, which Failsafe re-wraps in `FailsafeException` or propagates directly; unwrap `FailsafeException.getCause()` if needed).

Translation is centralized in `executeItemWithRetry` immediately after the Failsafe call; no Failsafe types escape that method.

### 4. Circuit breaker lifecycle: Failsafe `CircuitBreaker` instances held on the Pipeline

Failsafe's `CircuitBreaker<O>` is stateful and must persist across pipeline invocations (same as the current custom `CircuitBreakerRegistry`). The approach:

- At `Pipeline.build()`, a `Map<String, dev.failsafe.CircuitBreaker<?>>` replaces `CircuitBreakerRegistry`. Entries are created eagerly for every `StepNode` that has a non-null `CircuitBreakerPolicy`.
- Failsafe's `CircuitBreaker` is thread-safe by design — no additional locking needed.
- `CircuitBreaker.java` and `CircuitBreakerRegistry.java` are deleted.

### 5. Jitter: preserve current uniform-random semantics

Failsafe's built-in jitter uses `delay ± jitter%` (additive), which differs from FlowPipe's current `uniform(0, computedDelay)` (multiplicative truncation). To preserve exact behavioral semantics tested by existing tests, jitter is kept as a manual post-processing step on the Failsafe delay computation rather than using Failsafe's `.withJitter(...)`. Alternatively, if tests don't pin exact jitter distribution, Failsafe's jitter can be used — confirm during implementation.

### 6. Failsafe version and transitive deps

Use `dev.failsafe:failsafe:3.x` (latest stable 3.x release). Failsafe 3 has **zero transitive dependencies**. The version is pinned in `flowpipe-core/build.gradle.kts`; do not use a dynamic version range.

## Risks / Trade-offs

- **Jitter semantic shift** → Mitigate by checking whether existing tests assert on jitter distribution; if not, Failsafe's built-in jitter is fine. If yes, keep a manual jitter override.
- **Failsafe's timeout requires a scheduler thread** → Failsafe's `Timeout` policy uses `ForkJoinPool.commonPool()` by default for scheduling the interrupt. If this conflicts with the Lambda execution environment, supply an explicit `ScheduledExecutorService` via `Failsafe.with(...).with(scheduler)`. Document this as a configuration option.
- **Failsafe `FailsafeException` wrapping** → If a step throws a checked exception (impossible today since `Step.execute` signatures throw `Exception`), Failsafe wraps it in `FailsafeException`. Always unwrap via `.getCause()` before constructing `Failure`.
- **`CircuitBreakerPolicy` parameter mapping fidelity** → Failsafe's sliding window API may not map 1:1 to our `slidingWindowSize` + `minimumCalls` parameters. Verify exact builder methods during implementation and confirm behavioral tests still pass.

## Migration Plan

No migration plan needed — this is an internal implementation change. The public API and all behavioral contracts are unchanged. Existing tests validate correctness without modification. No deployment steps beyond a normal library release.

## Open Questions

- Does Failsafe 3.x's `CircuitBreaker` builder accept separate `slidingWindowSize` and `minimumCalls` parameters, or must they be set together? Verify against Failsafe 3.x Javadoc before coding the translation factory.
- Should jitter use Failsafe's built-in `.withJitter(Duration)` or keep a manual computation? Decide after checking what existing tests assert about jitter distribution.
