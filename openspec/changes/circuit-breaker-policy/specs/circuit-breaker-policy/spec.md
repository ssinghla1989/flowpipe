## ADDED Requirements

### Requirement: CircuitBreakerPolicy is an immutable value type
The library SHALL define `CircuitBreakerPolicy` as a public final class in `io.flowpipe.api` with five fields: `int failureRateThreshold` (1–100, inclusive), `int minimumCalls` (minimum 1), `int slidingWindowSize` (minimum equal to `minimumCalls`), `long openWindowMs` (minimum 0), and `int halfOpenProbeCount` (minimum 1). The class SHALL provide a static factory method `CircuitBreakerPolicy.of(int failureRateThreshold, int minimumCalls, int slidingWindowSize, long openWindowMs, int halfOpenProbeCount)` that validates all arguments and throws `IllegalArgumentException` on constraint violation. The class SHALL provide a `CircuitBreakerPolicy.defaults()` factory returning a policy with `failureRateThreshold=50`, `minimumCalls=5`, `slidingWindowSize=10`, `openWindowMs=60000`, `halfOpenProbeCount=2`.

#### Scenario: Valid policy is created with of()
- **WHEN** `CircuitBreakerPolicy.of(50, 5, 10, 60000, 2)` is called
- **THEN** the returned policy MUST have `failureRateThreshold()` equal to `50`, `minimumCalls()` equal to `5`, `slidingWindowSize()` equal to `10`, `openWindowMs()` equal to `60000`, and `halfOpenProbeCount()` equal to `2`

#### Scenario: failureRateThreshold below 1 throws IllegalArgumentException
- **WHEN** `CircuitBreakerPolicy.of(0, 5, 10, 60000, 2)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: failureRateThreshold above 100 throws IllegalArgumentException
- **WHEN** `CircuitBreakerPolicy.of(101, 5, 10, 60000, 2)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: minimumCalls less than 1 throws IllegalArgumentException
- **WHEN** `CircuitBreakerPolicy.of(50, 0, 10, 60000, 2)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: slidingWindowSize less than minimumCalls throws IllegalArgumentException
- **WHEN** `CircuitBreakerPolicy.of(50, 5, 4, 60000, 2)` is called
- **THEN** an `IllegalArgumentException` MUST be thrown

#### Scenario: defaults() returns a usable policy
- **WHEN** `CircuitBreakerPolicy.defaults()` is called
- **THEN** the returned policy MUST have `failureRateThreshold()` equal to `50`, `minimumCalls()` equal to `5`, `slidingWindowSize()` equal to `10`, `openWindowMs()` equal to `60000L`, and `halfOpenProbeCount()` equal to `2`

### Requirement: StepDescriptor carries a CircuitBreakerPolicy accessible to the engine
`StepDescriptor<I, O>` SHALL expose a `CircuitBreakerPolicy circuitBreakerPolicy()` accessor. `StepDescriptor` SHALL provide a `withCircuitBreaker(CircuitBreakerPolicy policy)` method that returns a new `StepDescriptor` with the specified policy set. Calling `withCircuitBreaker(null)` SHALL throw `NullPointerException`. When `withCircuitBreaker(...)` is not called, `circuitBreakerPolicy()` SHALL return `null` (no circuit breaker active for that step).

#### Scenario: Default circuit breaker policy is absent
- **WHEN** a `StepDescriptor` is constructed without calling `withCircuitBreaker(...)`
- **THEN** `descriptor.circuitBreakerPolicy()` MUST return `null`

#### Scenario: withCircuitBreaker sets the policy
- **WHEN** `descriptor.withCircuitBreaker(CircuitBreakerPolicy.defaults())` is called
- **THEN** the returned descriptor's `circuitBreakerPolicy()` MUST return the same policy instance

#### Scenario: withCircuitBreaker(null) throws NullPointerException
- **WHEN** `descriptor.withCircuitBreaker(null)` is called
- **THEN** a `NullPointerException` MUST be thrown

### Requirement: Circuit breaker state machine is CLOSED by default and transitions on failure rate
Each step with a non-null `CircuitBreakerPolicy` SHALL have an associated circuit breaker that starts in CLOSED state. The engine SHALL record the final outcome (success or failure) of each step execution (after any retry attempts). When the sliding window contains at least `minimumCalls` recorded outcomes AND the failure rate (failures / total calls in window) ≥ `failureRateThreshold / 100.0`, the circuit SHALL transition from CLOSED to OPEN. The failure rate calculation SHALL use only the most recent `slidingWindowSize` outcomes.

#### Scenario: Circuit opens after failure rate threshold is reached
- **WHEN** a step with `CircuitBreakerPolicy.of(50, 2, 4, 60000, 1)` fails on 2 consecutive calls (filling the minimum call count with 100% failure rate)
- **THEN** the circuit MUST transition to OPEN

#### Scenario: Circuit stays closed if failure rate is below threshold
- **WHEN** a step with `CircuitBreakerPolicy.of(50, 4, 4, 60000, 1)` alternates success and failure for 4 calls (50% failure rate, exactly at threshold)
- **THEN** the circuit MUST remain CLOSED (threshold requires failure rate strictly greater than or equal to 50%)

#### Scenario: Circuit stays closed if minimum calls not reached
- **WHEN** a step with `CircuitBreakerPolicy.of(50, 5, 10, 60000, 1)` fails on 4 consecutive calls (below minimumCalls of 5)
- **THEN** the circuit MUST remain CLOSED

### Requirement: OPEN circuit fast-fails calls with CircuitBreakerOpenException
When a step's circuit is OPEN, the engine SHALL NOT invoke `step.execute(...)` or the retry loop. Instead it SHALL immediately return a `Failure` whose `cause()` is a `CircuitBreakerOpenException` and whose `failedStepId()` is the step's id. `CircuitBreakerOpenException` SHALL be a public final class in `io.flowpipe.api` extending `RuntimeException`. It SHALL carry the step id and the earliest `Instant` at which the circuit may attempt to close (i.e., `openedAt + openWindowMs`).

#### Scenario: OPEN circuit returns Failure without calling execute
- **WHEN** a step's circuit is in OPEN state and the pipeline is invoked
- **THEN** `step.execute(...)` MUST NOT be called, the pipeline result MUST be a `Failure`, and `Failure.cause()` MUST be an instance of `CircuitBreakerOpenException`

#### Scenario: OPEN circuit Failure carries correct step id
- **WHEN** a step with id `"my-step"` has an OPEN circuit and the pipeline is invoked
- **THEN** `Failure.failedStepId()` MUST equal `"my-step"` and `((CircuitBreakerOpenException) failure.cause()).stepId()` MUST equal `"my-step"`

### Requirement: OPEN circuit transitions to HALF-OPEN after the open window expires
After `openWindowMs` milliseconds have elapsed since the circuit opened, the engine SHALL allow the next incoming call to proceed as a probe (transitioning the circuit to HALF-OPEN). Subsequent calls arriving while in HALF-OPEN and while probe slots remain SHALL also be allowed through. If all `halfOpenProbeCount` probes succeed, the circuit SHALL transition to CLOSED and reset the sliding window. If any probe fails, the circuit SHALL transition back to OPEN (resetting the open timestamp).

#### Scenario: Circuit transitions OPEN → HALF-OPEN after open window
- **WHEN** a step's circuit is OPEN and `openWindowMs` milliseconds have elapsed
- **THEN** the next pipeline invocation MUST call `step.execute(...)` (circuit is in HALF-OPEN probe mode)

#### Scenario: Successful probes close the circuit
- **WHEN** a step's circuit is HALF-OPEN with `halfOpenProbeCount=2` and both probe calls succeed
- **THEN** the circuit MUST transition to CLOSED and subsequent calls MUST proceed normally

#### Scenario: Failed probe reopens the circuit
- **WHEN** a step's circuit is HALF-OPEN and a probe call fails
- **THEN** the circuit MUST transition back to OPEN and the next call (before the open window expires again) MUST receive a `Failure` with `CircuitBreakerOpenException`

### Requirement: Circuit breaker state is per-Pipeline-instance, keyed by step id
The circuit breaker state for each step SHALL be held in a `CircuitBreakerRegistry` that is created during `Pipeline.build()` and owned by the `Pipeline` instance. State SHALL persist across multiple `pipeline.execute(...)` calls on the same instance. Different `Pipeline` instances SHALL have independent registries. The registry SHALL initialize a circuit breaker for a step lazily on first invocation, not at build time.

#### Scenario: Circuit state persists across multiple pipeline executions
- **WHEN** the same `Pipeline` instance is invoked multiple times and a step accumulates failures across those invocations
- **THEN** the failure count MUST accumulate across invocations (not reset per execution)

#### Scenario: Different Pipeline instances have independent circuit state
- **WHEN** two `Pipeline` instances are built from the same builder configuration and step A opens its circuit in instance 1
- **THEN** step A's circuit in instance 2 MUST remain CLOSED (state is not shared)

### Requirement: Circuit breaker outcome is based on the final retry result
When a step has both a `RetryPolicy` and a `CircuitBreakerPolicy`, the circuit breaker SHALL record the final outcome of the entire retry loop, not individual attempt outcomes. A retry loop that ultimately succeeds (even after intermediate failures) SHALL count as a single success. A retry loop that exhausts all attempts and fails SHALL count as a single failure.

#### Scenario: Transient failure recovered by retry counts as circuit success
- **WHEN** a step with `RetryPolicy.fixed(3, 0)` and a `CircuitBreakerPolicy` fails on the first attempt and succeeds on the second
- **THEN** the circuit breaker MUST record one success (not one failure and one success)

#### Scenario: Exhausted retry counts as one circuit failure
- **WHEN** a step with `RetryPolicy.fixed(3, 0)` and a `CircuitBreakerPolicy` fails on all three attempts
- **THEN** the circuit breaker MUST record one failure (not three)

### Requirement: Circuit breaker is invisible to step authors
Step authors SHALL implement only `describe()` and `execute(I, StepContext)`. The circuit breaker state machine SHALL be entirely engine-managed. No method, annotation, or marker interface related to circuit breaking SHALL be required or visible on the `Step` interface.

#### Scenario: Step interface has no circuit-breaker-related methods
- **WHEN** a developer implements `Step<I, O>`
- **THEN** the compiler MUST NOT require any circuit-breaker-related method beyond `describe()` and `execute(I, StepContext)`
