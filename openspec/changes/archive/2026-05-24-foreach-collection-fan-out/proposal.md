## Why

Real-world API pipelines routinely process collections — enriching a list of IDs, calling a downstream service per item, or transforming batches of records. Today, callers must hand-roll this fan-out themselves outside the pipeline boundary, losing all of FlowPipe's built-in validation, retry, metrics, and structured logging for each item. `foreach` closes this gap: it applies a typed `Step<I,O>` to every element of a `List<I>` and produces a `List<O>`, with bounded concurrency and the full observability stack applied to each iteration.

## What Changes

- New `PipelineBuilder.foreach(step)` and `PipelineBuilder.foreach(step, concurrency)` builder methods that accept a `Step<I,O>` when the current pipeline output type is `List<I>`, advancing the pipeline output type to `List<O>`.
- New `ForeachNode<I,O>` engine-internal sealed type that drives iteration with an `ExecutorService`-backed bounded thread pool (concurrency ≥ 1; default 1 = sequential).
- Build-time type check: `foreach` at position N must see a `List` output from position N−1 or it fails at `pipeline.build()` with a `PipelineBuildException`.
- Per-item observability: each item's execution emits `step.start` / `step.finish` / `step.error` structured log events and fires all three `MetricsRecorder` methods, exactly as a normal `StepNode` does.
- Per-item retry: `StepDescriptor.retryPolicy()` on the inner step is honoured for each item independently.
- Failure semantics: first item failure short-circuits the foreach (remaining futures cancelled), propagating a `Failure` with the inner step's id and the item's exception — consistent with how `ParallelNode` handles failures.
- `ExecutionTrace` records one `TraceEntry` per item, keyed `<stepId>[<index>]`.
- New spec `foreach-execution` covering the foreach contract.

## Capabilities

### New Capabilities
- `foreach-execution`: Iteration of a `Step<I,O>` over a `List<I>` producing `List<O>`, with configurable bounded concurrency, per-item retry, per-item observability, and fail-fast failure semantics.

### Modified Capabilities
*(none — existing step-execution, step-observability, and step-retry specs are unchanged; foreach builds on those contracts without altering them)*

## Impact

- `flowpipe-core` — `PipelineBuilder` gains two new `foreach` overloads; new `ForeachNode` sealed type added to `io.flowpipe.engine` package; `Pipeline.executeShared` handles `ForeachNode` alongside existing `StepNode`, `ParallelNode`, `BranchNode`.
- `flowpipe-test` — `Steps` utility and integration test suite extended with foreach examples.
- No new runtime dependencies; uses the existing `ExecutorService` already wired into `Pipeline`.
- No breaking changes to any existing public API.
