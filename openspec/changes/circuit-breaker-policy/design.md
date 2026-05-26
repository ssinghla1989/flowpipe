## Context

FlowPipe already has two per-step resilience policies — `RetryPolicy` (transparent re-invocation on failure) and `TimeoutPolicy` (per-attempt deadline). Both are per-invocation: they fire within a single request and leave no state behind when the request ends.

Circuit breaking is categorically different: it is stateful across requests. The breaker accumulates failure history over many calls and can suppress attempts entirely for a recovery window. This means the circuit breaker registry must live on the `Pipeline` instance and survive request boundaries.

The design targets the common case: a step that calls an external API that becomes degraded or unavailable. Without a circuit breaker, every request hammers the degraded service through the full retry budget, causing slow failures, resource exhaustion, and cascading load. With a circuit breaker, the pipeline fast-fails during the open window and gives the downstream system time to recover.

## Goals / Non-Goals

**Goals:**
- Implement the classic CLOSED → OPEN → HALF-OPEN state machine per step, keyed by step id.
- Declare the policy on `StepDescriptor` via `.withCircuitBreaker(CircuitBreakerPolicy)`, consistent with `withRetry` and `withTimeout`.
- Zero circuit-breaker code required from step authors.
- Thread-safe across concurrent pipeline invocations on the same `Pipeline` instance.
- No new runtime dependencies (build on `java.util.concurrent` primitives).

**Non-Goals:**
- Distributed circuit breaker state (no cross-JVM sharing; each `Pipeline` instance owns its own registry).
- Time-based sliding windows (count-based window only for v1; time-based is an extension).
- Per-exception-type failure classification (all `Throwable` from step execution count as failures; `ValidationException` from input/output validation does not).
- Metrics emission specific to circuit breaker state transitions (covered by existing `MetricsRecorder` SPI via `StepOutcome`).

## Decisions

### 1. State machine: count-based sliding window

The breaker evaluates the failure rate over the last `slidingWindowSize` completed calls. When the failure rate (failures / total calls) ≥ `failureRateThreshold` **and** the window has received at least `minimumCalls` calls, the circuit transitions CLOSED → OPEN.

**Why count-based over time-based:** Simpler to implement without wall-clock coupling, predictable in tests (controllable with a fixed call count), and sufficient for the primary use case (step that calls an external API). Time-based windowing is a future extension.

### 2. Registry scoped to the Pipeline instance

`CircuitBreakerRegistry` is created in `Pipeline.build()` and held as a field on the `Pipeline` instance. Each step id that has a `CircuitBreakerPolicy` gets its own `CircuitBreaker` entry, created lazily on first call.

**Why per-Pipeline-instance, not static/global:** A static registry would couple independent pipeline instances and make tests non-isolated. Per-instance matches the lifecycle of all other pipeline state (executor, metrics recorder, lifecycle hooks). On Lambda, each cold start creates a new Pipeline and a fresh registry — this is intentional: the Lambda instance starts with a clean circuit.

### 3. Execution order: circuit-check → retry-loop(timeout(execute))

The circuit is evaluated **before** the retry loop begins. If OPEN, the step is fast-failed immediately with `CircuitBreakerOpenException` — no retry, no timeout, no `execute` call. The circuit records the **final outcome of the retry loop** (success or all-attempts-exhausted failure), not each individual attempt. This keeps retry transparent to the circuit breaker and avoids opening the circuit on transient failures that retries would have recovered.

**Alternative considered — record each retry attempt:** Would cause the circuit to open faster under sustained transient failures. Rejected because it breaks the "retry is invisible" contract: a step with 3 retry attempts would count 3 failures against the circuit for a single logical call, making the `minimumCalls` threshold hard to reason about.

### 4. Thread safety via AtomicReference + AtomicInteger

Each `CircuitBreaker` holds:
- `AtomicReference<CircuitState>` for the CLOSED/OPEN/HALF-OPEN state (CAS transitions prevent races).
- A fixed-size ring buffer of `AtomicInteger` counts (or a `LongAdder`-based sliding window) for failure tracking in CLOSED.
- `AtomicInteger probeCount` for tracking HALF-OPEN probe calls.
- `volatile long openedAtMs` for the open-window timer.

No `synchronized` blocks; CAS-only for state transitions.

### 5. Half-open probing

When the open window expires (current time ≥ `openedAtMs + openWindowMs`), the first call CAS-transitions to HALF-OPEN. Up to `halfOpenProbeCount` calls are allowed through. If all probes succeed → CLOSED (reset counts). If any probe fails → OPEN (reset `openedAtMs`).

### 6. CircuitBreakerPolicy factory API

```java
CircuitBreakerPolicy.of(
    int failureRateThreshold,   // 1–100 (percent)
    int minimumCalls,           // >= 1
    int slidingWindowSize,      // >= minimumCalls
    long openWindowMs,          // >= 0
    int halfOpenProbeCount      // >= 1
)
```

A `CircuitBreakerPolicy.defaults()` convenience returns sensible values (50% threshold, 5 minimum calls, 10 sliding window, 60 000 ms open window, 2 probes).

### 7. CircuitBreakerOpenException placement

`CircuitBreakerOpenException` is a `RuntimeException` in `io.flowpipe.api`. It carries the step id and the earliest time the circuit may attempt to close (for observability/logging). It is the `cause` of the returned `Failure`.

## Risks / Trade-offs

- **Cold-start false positives on Lambda** — each Lambda instance starts with CLOSED and must accumulate `minimumCalls` before the circuit can open. This is expected; there is no warm-up risk. The risk is the opposite: a new instance has no memory of prior failures on the same external service. This is a known limitation of per-instance state; distributed state is a non-goal.
- **Count-based window is call-volume-sensitive** — on low-traffic steps (1–2 calls/minute), the window takes a long time to fill. Operators must set `minimumCalls` appropriately for their traffic pattern. Document this prominently.
- **No metrics SPI hooks for state transitions** — the existing `MetricsRecorder` SPI records `StepOutcome` (SUCCESS / FAILURE) per call, which implicitly captures circuit-open fast-fails as FAILURE. Dedicated circuit breaker metrics (state change events, probe results) are deferred; a future `CircuitBreakerListener` SPI can be added without breaking changes.
