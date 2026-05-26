## ADDED Requirements

### Requirement: Circuit breaker check precedes step invocation in the execution sequence
When a step's descriptor carries a non-null `CircuitBreakerPolicy`, the engine SHALL evaluate the step's circuit breaker state BEFORE initiating any retry loop or timeout wrapping. The complete execution sequence for a step with all three policies set SHALL be: (1) circuit breaker check — if OPEN, return `Failure` immediately; (2) retry loop begins; (3) within each retry attempt, timeout enforcement wraps `execute`; (4) `step.execute(input, ctx)` is called; (5) final retry outcome is recorded to the circuit breaker. Input and output validation continue to run inside the retry loop (before and after `execute`, respectively), unchanged.

#### Scenario: OPEN circuit short-circuits before retry and timeout
- **WHEN** a step with a `RetryPolicy`, `TimeoutPolicy`, and `CircuitBreakerPolicy` (circuit currently OPEN) is invoked
- **THEN** `step.execute(...)` MUST NOT be called, no retry sleep MUST occur, no timeout enforcement MUST occur, and the result MUST be a `Failure` with `CircuitBreakerOpenException` as its cause

#### Scenario: CLOSED circuit allows normal retry-and-timeout execution
- **WHEN** a step with a `RetryPolicy`, `TimeoutPolicy`, and `CircuitBreakerPolicy` (circuit currently CLOSED) is invoked
- **THEN** the retry loop MUST proceed normally and timeout enforcement MUST apply per attempt
