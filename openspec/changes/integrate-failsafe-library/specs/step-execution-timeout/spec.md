## ADDED Requirements

### Requirement: Failsafe TimeoutExceededException is translated to StepTimeoutException at the FlowPipe boundary
When Failsafe's `Timeout` policy detects a deadline breach and throws `dev.failsafe.TimeoutExceededException`, the engine SHALL catch that exception and translate it to `io.flowpipe.api.StepTimeoutException` carrying the correct `stepId` and `timeoutMs` before constructing the `Failure`. No `dev.failsafe.TimeoutExceededException` SHALL escape into `Failure.cause()` or any other part of the FlowPipe public API.

#### Scenario: Timeout breach surfaces StepTimeoutException, not TimeoutExceededException
- **WHEN** a step with `TimeoutPolicy.ofMillis(50)` blocks past its deadline and Failsafe enforces the timeout
- **THEN** `Failure.cause()` MUST be an instance of `io.flowpipe.api.StepTimeoutException` and MUST NOT be an instance of `dev.failsafe.TimeoutExceededException`

#### Scenario: Translated StepTimeoutException carries correct step metadata
- **WHEN** a step with id `"fetch-user"` and `TimeoutPolicy.ofMillis(100)` times out via Failsafe
- **THEN** `((StepTimeoutException) failure.cause()).stepId()` MUST equal `"fetch-user"` and `timeoutMs()` MUST equal `100`
