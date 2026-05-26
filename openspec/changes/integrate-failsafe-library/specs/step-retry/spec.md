## ADDED Requirements

### Requirement: Failsafe exception wrapping is transparent to callers
When the Failsafe retry executor wraps a step-thrown exception inside a `dev.failsafe.FailsafeException`, the engine SHALL unwrap via `FailsafeException.getCause()` before constructing the `Failure` result. Callers and `Failure.cause()` MUST see the original step-thrown `Throwable`, not a Failsafe wrapper type. No Failsafe type SHALL appear in the `io.flowpipe.api` or `io.flowpipe.engine` public surface.

#### Scenario: Step exception is not wrapped in FailsafeException
- **WHEN** a step with `RetryPolicy.fixed(2, 0)` throws a `RuntimeException` on every attempt
- **THEN** `Failure.cause()` MUST be the original `RuntimeException` instance, not a `dev.failsafe.FailsafeException`

#### Scenario: Retry count is still reported correctly via Failsafe
- **WHEN** a step with `RetryPolicy.fixed(3, 0)` fails on all three attempts
- **THEN** the `TraceEntry.attempts()` for that step MUST equal `3`, consistent with the Failsafe-tracked retry count
