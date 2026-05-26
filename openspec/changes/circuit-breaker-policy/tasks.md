## 1. Public API types

- [x] 1.1 Create `CircuitBreakerPolicy` final class in `io.flowpipe.api` with five fields (`failureRateThreshold`, `minimumCalls`, `slidingWindowSize`, `openWindowMs`, `halfOpenProbeCount`), `of(...)` factory with argument validation, and `defaults()` factory
- [x] 1.2 Create `CircuitBreakerOpenException` final class in `io.flowpipe.api` extending `RuntimeException`, carrying `stepId` and `retriableAfter` (`Instant`) accessors
- [x] 1.3 Add `CircuitBreakerPolicy circuitBreakerPolicy()` accessor and `withCircuitBreaker(CircuitBreakerPolicy)` builder method to `StepDescriptor`; default returns `null`; `withCircuitBreaker(null)` throws `NPE`

## 2. Engine internals

- [x] 2.1 Create `CircuitBreaker` package-private class in `io.flowpipe.engine` implementing the CLOSED/OPEN/HALF-OPEN state machine with a count-based ring-buffer sliding window; use `AtomicReference` for state and `AtomicInteger` for counters; no `synchronized`
- [x] 2.2 Create `CircuitBreakerRegistry` package-private class in `io.flowpipe.engine`; holds a `ConcurrentHashMap<String, CircuitBreaker>` keyed by step id; lazily initializes on first lookup; instantiated in `Pipeline.build()`
- [x] 2.3 Integrate circuit breaker check into the step execution path in `Pipeline`: before the retry loop, consult `CircuitBreakerRegistry` for the step's id; if OPEN, return `Failure(CircuitBreakerOpenException)` immediately; after the retry loop completes, record the final outcome to `CircuitBreaker.recordResult(boolean success)`

## 3. Validation and wiring

- [x] 3.1 Ensure `Pipeline.build()` instantiates `CircuitBreakerRegistry` and stores it as a final field; verify no per-request allocation occurs

## 4. Tests

- [x] 4.1 Unit-test `CircuitBreakerPolicy.of(...)` — valid construction, all invalid-argument branches (`failureRateThreshold` out of range, `minimumCalls < 1`, `slidingWindowSize < minimumCalls`, `openWindowMs < 0`, `halfOpenProbeCount < 1`)
- [x] 4.2 Unit-test `CircuitBreakerPolicy.defaults()` — assert all five field values
- [x] 4.3 Unit-test `StepDescriptor.withCircuitBreaker(...)` — default is `null`, setter round-trips, `null` argument throws `NPE`
- [x] 4.4 Integration-test CLOSED → OPEN transition: step with low `minimumCalls` fails enough times to reach the threshold; assert next call is `Failure` with `CircuitBreakerOpenException`
- [x] 4.5 Integration-test OPEN circuit: assert `execute` is not called and `Failure.failedStepId()` equals the step id
- [x] 4.6 Integration-test OPEN → HALF-OPEN → CLOSED: manipulate time (or use zero `openWindowMs`) so the window expires; assert probes are allowed through; assert successful probes close the circuit
- [x] 4.7 Integration-test OPEN → HALF-OPEN → OPEN: probe fails; assert circuit returns to OPEN
- [x] 4.8 Integration-test retry + circuit breaker: transient failure recovered by retry counts as one success to the circuit; all-attempts-exhausted counts as one failure
- [x] 4.9 Integration-test per-instance isolation: two `Pipeline` instances; open circuit on one; assert the other remains CLOSED
- [x] 4.10 Integration-test circuit breaker + timeout: OPEN circuit short-circuits before timeout enforcement; verify no `execute` call and no `InterruptedException`

## 5. Observability and documentation

- [x] 5.1 Confirm that existing `step.start` / `step.finish` / `step.error` SLF4J log events are NOT emitted for circuit-open fast-fails (no `execute` call was made); add a distinct `step.circuit_open` MDC log line at WARN level when a fast-fail occurs, including `stepId` and `retriableAfter`
- [x] 5.2 Update `CLAUDE.md` status section to list circuit breaker policy as a shipped feature
