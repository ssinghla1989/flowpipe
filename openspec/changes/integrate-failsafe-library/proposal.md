## Why

FlowPipe hand-rolls three resilience mechanisms — retry-with-backoff, per-attempt timeouts, and a circuit breaker state machine — each implemented from scratch with `java.util.concurrent` primitives. Failsafe is a battle-tested, production-grade Java resilience library that provides all three, with correct composition semantics (policy layering order, interaction between timeout/retry/circuit-breaker), thread safety, and edge-case handling. Replacing the custom implementations with Failsafe eliminates ~400 lines of bespoke concurrency code, reduces maintenance risk, and aligns with the in-flight circuit-breaker-policy change before it ships.

## What Changes

- **`dev.failsafe:failsafe`** added as a runtime dependency in `flowpipe-core`; the version is pinned and the dependency is an implementation detail — consumers are not expected to use Failsafe directly.
- Custom retry loop in `Pipeline` / `PipelineBuilder` replaced by a `Failsafe.with(RetryPolicy<O>)` execution; backoff, jitter, and attempt counting delegate entirely to Failsafe.
- Custom timeout mechanism (thread interruption via `ExecutorService.submit` + `Future.get`) replaced by Failsafe's `Timeout<O>` policy; `StepTimeoutException` is still thrown at the FlowPipe API layer (Failsafe's `TimeoutExceededException` is translated on the way out).
- Custom `CircuitBreaker` + `CircuitBreakerRegistry` state machines (introduced by the in-flight circuit-breaker-policy change) replaced by Failsafe's `CircuitBreaker<O>` keyed by step id on the `Pipeline` instance; `CircuitBreakerOpenException` is still thrown at the FlowPipe API layer.
- `CircuitBreaker.java` and `CircuitBreakerRegistry.java` (custom engine-internal classes) removed.
- Public API types — `RetryPolicy`, `TimeoutPolicy`, `CircuitBreakerPolicy`, `StepTimeoutException`, `CircuitBreakerOpenException` — remain unchanged; Failsafe types are never exposed to callers.
- Execution order stays: OPEN-circuit-check → timeout-enforced execute → retry loop → record circuit outcome.

## Capabilities

### New Capabilities

_(none — no new user-visible behavior is introduced)_

### Modified Capabilities

- `step-retry`: Implementation switches to Failsafe `RetryPolicy`; behavioral requirements are unchanged. Delta spec documents the Failsafe-backed translation of `RetryPolicy` fields and that exception wrapping from Failsafe is unwrapped before surfacing `Failure`.
- `step-execution-timeout`: Implementation switches to Failsafe `Timeout`; behavioral requirements are unchanged. Delta spec documents that `TimeoutExceededException` from Failsafe is translated to `StepTimeoutException` on the FlowPipe API boundary.

## Impact

- **`flowpipe-core/build.gradle.kts`**: New `implementation("dev.failsafe:failsafe:<version>")` dependency — first runtime dependency beyond `slf4j-api`.
- **`io.flowpipe.engine.Pipeline`** (and internal step-execution path): Retry loop, timeout future, and circuit breaker registry rewritten to use Failsafe policy objects composed via `Failsafe.with(...)`.
- **`io.flowpipe.engine.CircuitBreaker`**, **`io.flowpipe.engine.CircuitBreakerRegistry`**: Deleted.
- **Public API surface** (`io.flowpipe.api`): No changes to public types, signatures, or thrown exceptions.
- **Tests**: Existing retry, timeout, and circuit breaker tests remain valid (behavioral contracts are preserved); no test rewrites required unless timing expectations are thread-model sensitive.
- **Transitive dependency exposure**: Failsafe has no transitive dependencies, so this adds exactly one jar to the consumer's classpath.
