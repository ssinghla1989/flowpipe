## Why

FlowPipe steps that call external APIs can fail in sustained bursts — retries with backoff help individual calls recover, but they cannot prevent a pipeline from hammering a degraded downstream service for the entire duration of the outage. A circuit breaker detects sustained failure and fast-fails subsequent calls for a configurable recovery window, protecting both the caller and the downstream system.

## What Changes

- New `CircuitBreakerPolicy` value type, declared on `StepDescriptor` alongside `RetryPolicy` and `TimeoutPolicy`.
- The engine wraps each step execution in a circuit breaker state machine (CLOSED → OPEN → HALF-OPEN) keyed by step identity.
- When the circuit is OPEN, the step is bypassed immediately and a `Failure` with `CircuitBreakerOpenException` is returned — no `execute` call is made.
- Circuit state is held in a `CircuitBreakerRegistry` scoped to the `Pipeline` instance; it survives across requests so the breaker accumulates real failure history.
- `CircuitBreakerOpenException` is added to the public API surface (`io.flowpipe.api`).
- Step authors write zero circuit-breaker code; the policy is purely a framework concern.

## Capabilities

### New Capabilities

- `circuit-breaker-policy`: Configurable per-step circuit breaker policy with CLOSED/OPEN/HALF-OPEN state machine, failure-rate threshold, minimum call count, open window, and half-open probe count.

### Modified Capabilities

- `step-execution`: Steps now have a third policy slot (`CircuitBreakerPolicy`) evaluated before retry and timeout; the execution sequence is circuit-check → timeout → retry → step.

## Impact

- **`io.flowpipe.api`**: New public types `CircuitBreakerPolicy`, `CircuitBreakerOpenException`.
- **`StepDescriptor`**: New optional `withCircuitBreaker(CircuitBreakerPolicy)` builder method.
- **`io.flowpipe.engine`**: `Pipeline` and internal step-execution path updated to consult `CircuitBreakerRegistry` before delegating to retry/timeout machinery.
- **`flowpipe-test`**: `RecordingMetricsRecorder` and `StepHarness` are unaffected; no new test utilities required for the first cut.
- **No new runtime dependencies** — state machine implemented with `AtomicReference` and `AtomicInteger` on top of `java.util.concurrent`.
