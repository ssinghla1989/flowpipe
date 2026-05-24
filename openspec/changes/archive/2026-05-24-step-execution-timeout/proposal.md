## Why

When a step makes an external call (HTTP, database, downstream service) that hangs, the pipeline thread blocks indefinitely — there is no way to bound execution time per step today. On Lambda this exhausts the invocation budget silently; on EC2 it starves the thread pool under load.

## What Changes

- Introduce a `TimeoutPolicy` value type holding a duration (and an optional time unit).
- Add a `withTimeout(TimeoutPolicy)` method to `StepDescriptor<I, O>` so callers can attach a deadline to any step at wiring time.
- The engine enforces the timeout around every `step.execute(...)` call; if the deadline is exceeded, the step is interrupted and a `Failure` is produced whose `cause()` is a `StepTimeoutException` carrying the step id and the configured deadline.
- A timed-out step counts as a failure for the retry loop — if the step also has a `RetryPolicy`, each attempt gets its own independent deadline.
- Default behaviour (no `withTimeout(...)`) is unchanged: steps run without a time limit.

## Capabilities

### New Capabilities

- `step-execution-timeout`: The `TimeoutPolicy` value type, the `StepDescriptor.withTimeout(...)` wiring API, engine enforcement of the deadline around each attempt, interaction with the retry loop, and propagation of `StepTimeoutException` in the `Failure` result.

### Modified Capabilities

## Impact

- `flowpipe-core` — `io.flowpipe.api`:  new `TimeoutPolicy` class, new `StepTimeoutException`, updated `StepDescriptor` (new `withTimeout` method and `timeoutPolicy()` accessor).
- `flowpipe-core` — `io.flowpipe.engine`: engine step-invocation logic wraps `execute(...)` in a timed callable; timeout interacts with but does not change the retry loop contract.
- `flowpipe-test` — `Steps` utility may gain convenience helpers for easily building steps that sleep (to exercise timeout tests).
- No new runtime dependencies; `Thread.interrupt()` / `Future.get(timeout)` are standard JDK.
