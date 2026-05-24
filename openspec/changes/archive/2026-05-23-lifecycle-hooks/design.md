## Context

FlowPipe already has two framework-level extension points registered on `PipelineBuilder`: `MetricsRecorder` (step-level metrics) and `ExecutorService` (parallel dispatch). Both follow the same pattern: a SPI registered via a single `.withX(...)` builder method, stored on the built `Pipeline`, invoked by the engine around execution.

Lifecycle hooks extend this pattern with a third SPI — `PipelineLifecycle<I, O>` — that fires at the pipeline boundary rather than around individual steps. The engine already contains all the dispatch machinery needed; the change is additive.

## Goals / Non-Goals

**Goals:**
- Provide a named `PipelineLifecycle<I, O>` SPI with three callback points: `onStart`, `onFinish`, `onError`.
- Integrate into `PipelineBuilder` via `.withLifecycle(PipelineLifecycle<I, O>)`, consistent with `.withMetrics(...)`.
- Give hooks access to `StepContext` (State + RequestContext) so they can seed or read mutable execution state.
- Fail-fast on `onStart` exceptions (promote to `Failure`); isolate `onFinish`/`onError` exceptions (log, never mask the pipeline result).
- Add `RecordingPipelineLifecycle` to `flowpipe-test` for unit testing hook invocations.

**Non-Goals:**
- Step-level hooks — `MetricsRecorder` already covers per-step observability.
- Hook ordering / chaining — a single lifecycle instance per pipeline is sufficient.
- Async hook execution — hooks run synchronously on the caller's thread.
- Modifying pipeline input or output from within a hook — hooks are observers only.

## Decisions

### Decision 1: SPI interface with default no-op methods, not a separate NoOp class

`PipelineLifecycle<I, O>` is a public interface whose three methods carry `default` no-op bodies. Implementors override only the callbacks they care about.

**Alternatives considered:**
- **`abstract` class with empty bodies**: Prevents composition via multiple interfaces; offers no advantage here.
- **Separate `NoOpPipelineLifecycle` class** (matching the `NoOpMetricsRecorder` pattern): Forces unnecessary boilerplate — implementors extending it cannot also extend another class, and they cannot selectively override just `onStart`. Default methods avoid this.

**Why default methods here but not on `MetricsRecorder`?**: `MetricsRecorder` has four fine-grained recording methods that must all be implemented consistently for metrics to be coherent. Lifecycle hooks have only three independent callbacks; a no-op for any single one is always safe.

### Decision 2: Hooks receive `StepContext`, not separate `(State, RequestContext)` parameters

All three callbacks receive `(input/result, StepContext ctx)`. `StepContext` exposes both `ctx.state()` (mutable) and `ctx.context()` (immutable `RequestContext`).

**Why**: `onStart` is the most natural place to seed shared state that steps downstream will read (e.g., initialise a distributed-trace span and store the span ID in `State`). `onFinish` and `onError` can then retrieve that span ID without re-extracting it from `RequestContext`. Passing `StepContext` is consistent with the step-author API and avoids introducing a parallel "hook context" type.

**Constraint**: The same `State` and `RequestContext` instance used throughout the pipeline execution is passed to hooks, giving them a live view — not a snapshot.

### Decision 3: `onStart` failures become `Failure`; `onFinish`/`onError` failures are logged and isolated

- If `onStart(input, ctx)` throws, the pipeline returns a `Failure` immediately with `failedStepId = "pipeline.onStart"`. No steps run.
- If `onFinish(result, ctx)` throws, the exception is logged at `WARN` level (same as `safeRecord` in `emitRecord`). The pipeline's actual result is returned unchanged.
- If `onError(failure, ctx)` throws, same isolation: logged at `WARN`, failure result returned unchanged.

**Rationale**: `onStart` is a gate — an auth check that throws is intended to block execution. Post-execution hooks are observers; masking a `Success` result because `onFinish` threw would be surprising and hard to debug.

### Decision 4: `onFinish` is always called; `onError` is a convenience called only on `Failure`

Call order on success: `onStart` → steps → `onFinish(Success, ctx)`.
Call order on failure: `onStart` → steps → `onFinish(Failure, ctx)` → `onError(Failure, ctx)`.

**Why both `onFinish` and `onError`?**: `onFinish` is the right place for cleanup that must always run (resource release, span finalisation). `onError` is the right place for failure-specific side effects (alerting, dead-letter routing) without forcing users to pattern-match `Result` inside `onFinish`.

### Decision 5: Dispatch lives in `Pipeline.execute(...)`, not in a new wrapper type

The hook calls are added to the top of `Pipeline.executeShared(...)` (for `onStart`) and wrapped around it (for `onFinish`/`onError`). No new delegation layer or proxy type is introduced. This keeps the call stack shallow and the engine's existing `executeShared` path unchanged for sub-pipelines (branch arms, etc.) — hooks fire only at the outermost `execute(...)` call.

**Why not wrap `executeShared`?**: `executeShared` is also called by branch arms and parallel dispatch, where lifecycle hooks must NOT fire — only the top-level pipeline boundary is a lifecycle boundary.

## Risks / Trade-offs

- **`onStart` as a hard gate**: If the `onStart` implementation has a bug (e.g., NullPointerException), it silently blocks all pipeline executions. Mitigation: wrap `onStart` in the same `safeRecord`-style try/catch that logs and re-throws only intentional `Failure` paths; document clearly that `onStart` is a deliberate gate.
- **`StepContext` mutability in `onFinish`**: Hook authors can write to `State` after all steps have run. This is intentionally allowed (needed for span finalisation) but could be surprising if a pipeline is inspected externally. No mitigation needed — the `State` object is execution-scoped and discarded after `execute(...)` returns.
- **Single lifecycle instance**: Chaining multiple lifecycle implementations requires the user to compose them manually. Acceptable for now; a `PipelineLifecycle.compose(...)` factory can be added later without breaking changes.

## Open Questions

None — design is fully specified.
