## Why

Every REST handler that uses FlowPipe currently has no standard place to run cross-cutting logic — auth verification, audit logging, distributed-trace finalization, or resource cleanup — that spans the entire pipeline rather than any individual step. Without lifecycle hooks, callers must wrap `pipeline.execute(...)` in ad-hoc try/finally boilerplate that duplicates the same pattern everywhere.

## What Changes

- Introduce a `PipelineLifecycle<I, O>` interface (SPI) with three optional callback points: `onStart`, `onFinish`, and `onError`.
- Expose `.withLifecycle(PipelineLifecycle<I, O>)` on `PipelineBuilder`, following the same pattern as `.withMetrics(...)`.
- The engine calls hooks in a defined order around every pipeline execution; hook exceptions are surfaced as `Failure` without masking the original cause.
- Hook authors receive `RequestContext` (read-only) and the pipeline input/result — they cannot mutate either.
- Hook authors write zero logging and zero metrics code; the framework instruments hooks the same way it instruments steps.

## Capabilities

### New Capabilities

- `pipeline-lifecycle`: The `PipelineLifecycle<I, O>` SPI, how hooks are registered via `PipelineBuilder`, when each callback fires, what arguments each receives, exception-handling semantics, and the guarantee that hook authors need no instrumentation code.

### Modified Capabilities

- `pipeline-composition`: The `PipelineBuilder` gains a `.withLifecycle(PipelineLifecycle<I, O>)` registration method, following the existing pattern of `.withMetrics(...)` and `.withExecutor(...)`.

## Impact

- New public type: `io.flowpipe.api.PipelineLifecycle<I, O>` (SPI interface).
- `PipelineBuilder` gains one new method; no existing methods change.
- `Pipeline` engine gains hook-dispatch logic wrapping every `execute(...)` call.
- `flowpipe-test` gains a `RecordingPipelineLifecycle` (mirrors `RecordingMetricsRecorder`) for unit-testing hook invocations.
- No Spring, no external dependencies, no changes to step authors' contract.
