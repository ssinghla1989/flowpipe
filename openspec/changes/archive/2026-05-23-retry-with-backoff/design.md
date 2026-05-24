## Context

Today every `StepNode` execution is a single try: on any `Throwable` the engine records a `Failure` and stops. There is no recovery path. Callers who need retry logic must either wrap their `Step.execute` body themselves — leaking infrastructure concern into step code — or accept that transient failures become pipeline failures.

The engine's `StepNode` is the natural insertion point: it already owns the try/catch that converts exceptions into `Failure`, and it receives the fully-resolved `StepDescriptor` that carries per-step configuration. Adding a retry loop there keeps step authors completely isolated.

## Goals / Non-Goals

**Goals:**

- Configurable per-step retry with exponential backoff and optional jitter.
- Step authors write zero retry-related code; `execute` is called once per attempt.
- Retry attempts are observable: `step.retry` log event and updated `MetricsRecorder` `recordStepAttempts` call reflect the actual attempt count.
- `RetryPolicy.none()` default preserves identical behaviour for all existing steps.
- Works unchanged on AWS Lambda (no background threads, no daemon threads).

**Non-Goals:**

- Pipeline-level retry (retry the whole pipeline on failure).
- Retry budgets / rate-limited retry across multiple steps.
- Circuit breakers or half-open states.
- Retry predicates (deciding which exceptions to retry on — max-attempts is the only stop condition in this slice; exception filtering can be added later without breaking the API).
- Async / non-blocking backoff.

## Decisions

### 1. `RetryPolicy` is an immutable value type declared on `StepDescriptor`

**Decision**: `RetryPolicy` is a standalone public type in `io.flowpipe.api`. `StepDescriptor<I, O>` gets `RetryPolicy retryPolicy()` alongside the existing `inputType()`, `outputType()`, and validator accessors. Builders get `.withRetry(RetryPolicy)`.

**Rationale**: Retry is a per-step concern (step A may be idempotent and retry-safe; step B may not). Putting it on the descriptor keeps it discoverable at wiring time and validates it in `pipeline.build()`. A pipeline-level default could be layered on top later without changing this surface.

**Alternative rejected**: Global `PipelineBuilder.withDefaultRetry(...)` — this hides per-step intent and forces a different policy to override it every time. Makes reading a pipeline harder.

### 2. `RetryPolicy` fields: `maxAttempts`, `initialDelayMs`, `multiplier`, `jitter`

**Decision**: `RetryPolicy` carries four fields:
- `int maxAttempts` — total attempts including the first (1 = no retry, 2 = one retry, …).
- `long initialDelayMs` — sleep before the second attempt.
- `double multiplier` — factor applied to delay on each subsequent attempt (1.0 = constant, 2.0 = exponential doubling).
- `boolean jitter` — when true, sleep time is `uniform(0, computedDelay)` to avoid thundering herd.

**Rationale**: These four cover the vast majority of real-world retry shapes. `maxAttempts=1` (the `none()` default) requires zero sleeping and is safe on Lambda.

**Alternative rejected**: Builder-style `RetryPolicy.builder()` API — the field count is small enough that a static factory per use case (`RetryPolicy.exponential(3, 200)`) is more readable. Builders can be added later.

### 3. Retry loop lives inside `StepNode`, wrapping the existing try/catch

**Decision**: `StepNode.execute(...)` gains an outer loop `for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++)`. On each `Throwable` it checks whether another attempt is allowed; if yes it emits `step.retry`, sleeps, and continues; if no it converts to `Failure` as before.

**Rationale**: Keeps all retry state inside `StepNode`. The `Pipeline` execution loop and `ParallelNode` are unaware of retry — they receive either a successful output or a `Failure` exactly as today. Validation (input/output) runs fresh each attempt because the retry loop wraps the full validate→execute→validate sequence.

**Alternative rejected**: Retry in the `Pipeline` execution loop — would require exposing retry state upward and making the loop aware of which failures are retriable vs. terminal.

### 4. `recordStepAttempts` carries actual attempt count; new `recordRetryAttempt` callback for per-attempt signals

**Decision**: After all attempts are exhausted (or success), `recordStepAttempts(stepId, actualAttemptCount)` is called with the true count (≥1). A new `MetricsRecorder` method `recordRetryAttempt(String stepId, int attemptNumber)` is added to the SPI so metric backends can track per-attempt events without having to infer them from the final count. `NoOpMetricsRecorder` adds a no-op implementation; `RecordingMetricsRecorder` in `flowpipe-test` captures the calls.

**Rationale**: Separating the per-attempt hook from the final count lets backends emit a histogram of retry attempts while also recording a count gauge. The final count alone is insufficient for latency-per-attempt metrics.

**Alternative rejected**: Adding only the final count and letting backends track the rest — insufficient for real-time retry dashboards.

## Risks / Trade-offs

- **`Thread.sleep` blocks the calling thread**: On Lambda, the entire function invocation is on one thread. A retry with 3 attempts × 1s delay = up to 2s of added latency. This is acceptable and documented — callers on Lambda should set `maxAttempts=1` or use short delays. There is no async path.
- **Retry amplifies load on downstream services**: A fleet of Lambda functions all retrying simultaneously can spike a downstream API. The `jitter` flag mitigates this but doesn't eliminate it. This is a product-level concern, not a library concern.
- **Input validation runs on every attempt**: If the input validator is expensive, retries multiply that cost. This is correct behaviour — a validator might be stateful (e.g., checking a token expiry) — and is documented.
- **`MetricsRecorder` SPI gains a new method**: Existing `MetricsRecorder` implementations outside the library will fail to compile after this change. This is an intentional breaking change to the SPI (documented). `NoOpMetricsRecorder` and `RecordingMetricsRecorder` are updated; users with custom recorders must add the method.

## Migration Plan

1. `RetryPolicy.none()` is the default for all steps with no `withRetry(...)` call — zero behavioural change for existing pipelines.
2. Existing custom `MetricsRecorder` implementations must add `recordRetryAttempt(String, int)` with a no-op body to compile. The method signature is simple and the change is purely additive to the call graph.
3. No data migration, no configuration changes, no deployment coordination required.

## Open Questions

- Should `RetryPolicy` eventually support an exception predicate (`Predicate<Throwable> retryOn`) so callers can restrict retries to specific exception types (e.g., only `IOException`)? Deferred to a follow-on change to avoid scope creep.
- Should there be a `RetryPolicy.fixed(int maxAttempts, long delayMs)` convenience factory (no multiplier)? Yes — add alongside `none()` and `exponential(...)` in the implementation.
