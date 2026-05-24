## Why

Steps that call downstream APIs will transiently fail — rate limits, timeouts, brief unavailability — and today the pipeline immediately surfaces the failure to the caller with no recovery attempt. Retrying with backoff is the single most impactful reliability improvement a pipeline framework can offer, and it belongs in the framework so every step gets it for free.

## What Changes

- Introduce a `RetryPolicy` value type that encodes max-attempts, initial delay, backoff multiplier, and jitter strategy.
- Add `.withRetry(RetryPolicy)` to `StepDescriptor` so per-step retry behaviour is declared at wiring time.
- The engine wraps each `StepNode` execution in retry logic driven by the policy; step authors see exactly one `execute` call per logical attempt (the framework handles re-invocation).
- Failed attempts that are retried emit `step.retry` structured log events and record attempt counts through `MetricsRecorder`.
- A `RetryPolicy.none()` factory (equivalent to no-op, zero retries) ensures backwards-compatible defaults.

## Capabilities

### New Capabilities

- `step-retry`: Per-step configurable retry-with-backoff — `RetryPolicy` type, `StepDescriptor.withRetry(...)`, engine retry loop, `step.retry` observability events, and `MetricsRecorder` retry hooks.

### Modified Capabilities

- `step-observability`: New `step.retry` structured log event and retry-specific `MetricsRecorder` callbacks added alongside existing `step.start` / `step.finish` / `step.error`.

## Impact

- **`io.flowpipe.api`** — new `RetryPolicy` public type; `StepDescriptor` gains `.withRetry(RetryPolicy)`.
- **`io.flowpipe.engine`** — `StepNode` and pipeline execution loop updated to honour retry policy; `DefaultStepContext` unaffected (step authors are isolated from retry mechanics).
- **`io.flowpipe.observability`** — `MetricsRecorder` SPI extended with retry-event callback; `NoOpMetricsRecorder` updated to default no-op.
- **`flowpipe-test`** — `RecordingMetricsRecorder` extended to capture retry events; `StepHarness` may expose retry count for assertions.
- No new runtime dependencies. Retry loop uses `Thread.sleep`; callers on Lambda can set `maxAttempts=1` to opt out of all sleeping.
