## Why

The Phase 1 foundations established the `Step → Pipeline → Result` plumbing with build-time validation, but the engine currently runs steps silently — no logs, no metrics, only the in-memory `ExecutionTrace`. One of FlowPipe's headline guarantees is that *step authors write zero logging code and zero metrics code* — both are framework concerns wrapped around `execute`. That guarantee is unmet until the engine actually emits structured logs and metrics on every step boundary. Phase 2 closes that gap.

## What Changes

- Add an internal SLF4J logger to the execution engine that emits three structured events per step invocation: `step.start`, `step.finish` (success), and `step.error` (any thrown / validation failure).
- Each log event SHALL carry structured key-value fields: `step.id`, `step.attempt` (always `1` until retry lands in Phase 4), `step.duration_ms` on terminal events, `step.outcome` (`success` / `failure`) on terminal events, and `step.error_class` / `step.error_message` on `step.error`. Engine logs also carry all `RequestContext` entries as structured fields (key = `ContextKey.name()`).
- Introduce a `MetricsRecorder` SPI in `io.flowpipe.observability` exposing three emission methods covering step duration, step attempt count, and step outcome. Ship a `NoOpMetricsRecorder` as the default.
- Extend `PipelineBuilder` with `.withMetrics(MetricsRecorder)` to wire a recorder onto a built pipeline. Default: `NoOpMetricsRecorder.instance()`.
- Extend `Pipeline.execute(...)` with an overload that accepts a per-execution `MetricsRecorder`, overriding the build-time default for that one call (useful in tests). Existing `execute(input)` and `execute(input, context)` overloads SHALL continue to work and use the build-time recorder.
- The engine SHALL invoke the recorder on every terminal step event, in the same flow that appends the `TraceEntry`. Recorder exceptions SHALL be caught and logged — they MUST NOT affect the pipeline result.
- Add a `RecordingMetricsRecorder` to `flowpipe-test` so consumers can assert metric emission in their own tests.
- No new runtime dependencies. SLF4J is already on the classpath; `MetricsRecorder` is in-house. Logback added as a `testImplementation` only, so SLF4J emission is observable in test assertions.

## Capabilities

### New Capabilities
- `step-observability`: Defines the structured logging and metrics emission the engine performs around every step invocation, the `MetricsRecorder` SPI consumers plug into, how the recorder is wired (build-time default, per-execution override), and the contract that recorder/logger failures are isolated from pipeline outcomes.

### Modified Capabilities
<!-- None. The new observability behavior is purely additive; existing requirements in `step-execution`, `pipeline-composition`, `pipeline-result`, and `execution-state` continue to hold unchanged. -->

## Impact

- **`flowpipe-core` source**: New package `io.flowpipe.observability` with `MetricsRecorder`, `NoOpMetricsRecorder`, and `StepOutcome` enum. Existing `Pipeline` and `PipelineBuilder` in `io.flowpipe.engine` gain observability wiring. Engine execution loop wraps each step with logger calls and recorder calls.
- **`flowpipe-test` source**: New `RecordingMetricsRecorder` that captures every emission for assertion.
- **Dependencies**: Logback added as `testImplementation` in `flowpipe-core` for SLF4J emission to be observable; no production-runtime additions. `slf4j-api` stays the sole runtime dep.
- **Documentation**: Update `CLAUDE.md` "Module and package layout" to list `io.flowpipe.observability`. Update `README.md` example to show plugging in a `MetricsRecorder`.
- **Downstream changes unblocked**: Phase 4 (retry + lifecycle hooks) will populate `step.attempt` with the real attempt count and add `pipeline.error` / `pipeline.finish` aggregate events on top of the per-step events shipped here.
- **No breaking changes** — every existing API is preserved. New overloads and a new builder method are purely additive.
