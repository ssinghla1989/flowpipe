## Context

Phase 1 produced a synchronous execution engine that captures every step's id, start time, duration, and attempt count in an in-memory `ExecutionTrace` attached to the returned `Result`. That trace is useful for tests but invisible in production: nothing flows to logs, nothing flows to a metrics backend. This change adds both, behind a stable SPI that lives in core (so consumers can plug Micrometer, OpenTelemetry, Prometheus client, etc. later through their own adapters).

Two hard constraints from the project's `CLAUDE.md` shape every decision below:
- **Step authors write zero logging code and zero metrics code.** The engine emits both around `execute`, never inside it.
- **No external runtime assumptions / Lambda-safe.** No background threads, no static mutable carriers, no daemon resources.

## Goals / Non-Goals

**Goals:**
- Every step boundary emits a structured `step.start` log, plus exactly one of `step.finish` (success) or `step.error` (failure), with consistent field names.
- A `MetricsRecorder` SPI consumers can implement to forward step metrics to any backend, with a no-op default.
- Consumer ergonomics: wire a recorder once at build time (`PipelineBuilder.withMetrics(...)`); override per execution where useful (testing, A/B).
- Recorder and logger failures are fully isolated — no observability bug can turn a successful pipeline into a `Failure`.
- Zero new mandatory runtime dependencies in `flowpipe-core`.

**Non-Goals:**
- Pipeline-aggregate events (`pipeline.start`, `pipeline.finish`). Per-step events are sufficient until retry/hooks (Phase 4) introduce a meaningful pipeline-level outcome story.
- Sampling, rate limiting, log-level configuration. SLF4J configuration is the consumer's concern.
- A Micrometer adapter module (`flowpipe-micrometer`). Scoped out — comes after the SPI stabilises.
- Tracing spans / OpenTelemetry integration. Same reason.
- Automatic propagation of `RequestContext` into step-internal logs (see D4 below — deliberately kept manual for now).
- Changes to the synchronous, single-threaded engine model.

## Decisions

### D1: SLF4J `org.slf4j.event.KeyValuePair` style structured fields (fluent API)

Each engine log call uses SLF4J 2.x's fluent API: `logger.atInfo().setMessage("step.start").addKeyValue("step.id", id).addKeyValue("step.attempt", 1).log()`. This emits structured key-value pairs that Logback ≥ 1.4 and any other SLF4J-2-compatible backend can serialise to JSON or key=value text. Plain `logger.info("step.start id={} attempt={}", id, 1)` would also work for humans but loses machine-readability.

`slf4j-api:2.0.13` is already the sole runtime dep. No additions required.

**Alternatives considered:** Logback's encoder-specific markers (rejected — couples core to Logback). Print formatting (rejected — not machine-parseable).

### D2: `MetricsRecorder` SPI — three methods, no batching, no scopes

```java
public interface MetricsRecorder {
    void recordStepDuration(String stepId, long durationNanos);
    void recordStepAttempts(String stepId, int attempts);
    void recordStepOutcome(String stepId, StepOutcome outcome);
}
```

`StepOutcome` is a two-value enum (`SUCCESS`, `FAILURE`) — sealed via enum naturally.

Three methods rather than one fat `record(StepEvent)` because: (a) it lets adapters map each metric to the natural primitive in their backend (histogram, counter, counter-by-label respectively) without unpacking; (b) recorder implementations can no-op individual methods cheaply.

No batching API and no scope/handle pattern — keep the SPI minimal until a real adapter shows up demanding more.

**Alternatives considered:** Single `record(StepMetric)` method (rejected — pushes type discrimination onto every adapter implementer for no benefit). Builder/scope pattern à la Micrometer's `Timer.Sample` (rejected — adds API surface a no-op default can't usefully implement; revisit when a real adapter lands).

### D3: Wiring — `PipelineBuilder.withMetrics(recorder)` build-time, `execute(input, ctx, recorder)` per-call override

Build-time wiring is the normal path — for Lambda and EC2 services, the recorder is a process-wide singleton (Micrometer, statsd) baked in once. Per-call override exists for tests and for consumers wanting per-tenant recorders without rebuilding the pipeline. If both are present, the per-call recorder wins for that execution only; the pipeline's default is untouched for subsequent calls.

Logger wiring needs no public configuration — engine uses `LoggerFactory.getLogger(Pipeline.class)`. Consumers configure SLF4J backend the standard way.

**Alternatives considered:** Global static `Pipeline.setDefaultRecorder(...)` (rejected — static mutable state, violates the architectural-test invariant). Per-step recorder configuration (rejected — premature; the same recorder always handles all steps in a pipeline). Constructor injection on `Pipeline` directly (rejected — `Pipeline` is built by `PipelineBuilder`; the builder is the right wiring seam).

### D4: `RequestContext` fields are added to engine logs as structured KVs; step-internal logs are NOT auto-tagged

Every engine-emitted log line (`step.start`, `step.finish`, `step.error`) gets every `RequestContext` entry added as a structured key-value pair, keyed by the `ContextKey.name()`. This way `traceId`, `tenantId`, etc. flow through observability without any step author involvement.

We deliberately do **not** push `RequestContext` into SLF4J's `MDC` for the duration of `step.execute`. MDC is ThreadLocal-backed and would auto-tag logs the step author emits — but the architectural invariant in [execution-state](../../specs/execution-state/spec.md) prohibits thread-local state for execution data, and quietly exempting MDC would erode the rule. Step authors who want their own logs tagged can read `ctx.context().get(TRACE_ID)` and call `MDC.put` themselves; the framework doesn't force a global mechanism on them.

**Alternatives considered:** MDC push/pop around `execute` (rejected — see above; thread-local execution data is a stated non-goal). Treat `RequestContext` opaquely and log only `step.id` (rejected — defeats the value of the request-context tier).

### D5: Recorder + logger exception isolation — caught, logged, swallowed

`try { recorder.recordX(...); } catch (Throwable t) { logger.atWarn().log("metrics recorder failed", t); }` around each recorder call. SLF4J calls inside the engine are themselves wrapped in `try/catch` only for the recorder, not the logger; if SLF4J's own logger.log throws, that's a misconfiguration the consumer needs to see — let it propagate. (In practice SLF4J's `Logger` interface does not throw from logging methods.)

**Alternatives considered:** Let recorder exceptions propagate (rejected — turns an instrumentation bug into a `Failure`, completely violating the "observability is invisible to step authors" promise). Disable a recorder after first failure (rejected — silent state, hard to debug).

### D6: Where exactly emission lives in the execution loop

```
for each step:
    log step.start
    startNanos = nanoTime()
    try:
        validate input
        out = step.execute(...)
        validate output
        duration = nanoTime() - startNanos
        traceBuilder.append(TraceEntry(stepId, startNanos, duration, attempts=1))
        recorder.recordStepDuration(stepId, duration)        // wrapped
        recorder.recordStepAttempts(stepId, 1)               // wrapped
        recorder.recordStepOutcome(stepId, SUCCESS)          // wrapped
        log step.finish (with duration_ms, outcome=success)
    catch t:
        duration = nanoTime() - startNanos
        traceBuilder.append(...)
        recorder.recordStepDuration(stepId, duration)        // wrapped
        recorder.recordStepAttempts(stepId, 1)               // wrapped
        recorder.recordStepOutcome(stepId, FAILURE)          // wrapped
        log step.error (with duration_ms, outcome=failure, error_class, error_message)
        return Failure(t, stepId, traceBuilder.build())
```

Crucial property: **duration is identical** in the trace entry, in the metric, and in the log line — all computed from the same `startNanos`. No double-instrumentation drift.

Emission order — trace append, then recorder calls, then log — is fixed and tested. The trace is the source of truth; recorder/log are projections of it.

### D7: `RecordingMetricsRecorder` in `flowpipe-test` for consumer tests

Captures every call as an immutable record in an internal list. Exposes `events()`, `events(stepId)`, and convenience predicates. Pure test ergonomics; not part of the core SPI's behavior.

## Risks / Trade-offs

- **Recorder is called three times per step (duration + attempts + outcome)**, which is fine for a no-op recorder but is a microbenchmark concern under hot loads with a non-trivial backend. → Mitigation: documented as a deliberate trade-off; adapters can no-op the methods they don't care about. Revisit if a real backend's adapter measures meaningful overhead.
- **Per-execution recorder override lives on the existing `execute` overload**, growing the API surface. → Mitigation: only one new overload (`execute(I, RequestContext, MetricsRecorder)`); the no-arg and context-only forms continue to delegate to the build-time recorder.
- **`RequestContext` fields are added to every engine log line**, which can balloon log volume if a consumer puts large objects into the context. → Mitigation: SLF4J's structured logging is lazy — fields are not serialised by default until a backend asks for them. We can revisit with a `loggable()` predicate on `ContextKey` if real consumers hit the issue.
- **Logback shipped as `testImplementation` of `flowpipe-core`**, which makes `flowpipe-core` tests slightly heavier and creates a small risk of relying on Logback-specific behavior in tests. → Mitigation: tests assert against SLF4J 2.x `KeyValuePair`s captured by an in-memory appender, not Logback formatting. Choosing Logback over `slf4j-simple` because it has a usable in-memory `ListAppender` for assertions.
- **Hardcoding step-level emission means the engine cannot easily emit pipeline-aggregate events later** without adding a second emission point. → Mitigation: explicitly out of scope here; Phase 4 introduces lifecycle hooks which naturally absorb aggregate emission.

## Migration Plan

Strictly additive. No existing test, consumer, or API call needs to change.

- `Pipeline.execute(I)` and `Pipeline.execute(I, RequestContext)` behave identically to before, except they now emit logs and call the recorder. With the default `NoOpMetricsRecorder`, recorder calls are unobservable from the test surface.
- Existing pipelines built without `.withMetrics(...)` automatically pick up the no-op recorder. No surprise emission.

## Open Questions

None that block this change. Three to revisit later:
1. Whether to add a `loggable()` predicate on `ContextKey` so consumers can mark some context entries as "do not log" (PII, large blobs). Wait for a real consumer to ask.
2. Whether `recordStepOutcome` should also accept the `failedStepId` cause (currently only on the log). The recorder usually wants counter cardinality bounded; passing causes risks unbounded labels. Defer.
3. Whether to ship a `Slf4jMetricsRecorder` adapter that just emits metrics as logs. Nice for getting started without a metrics backend, but easy to add later if asked.
