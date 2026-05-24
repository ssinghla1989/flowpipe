## Context

FlowPipe supports sequential, parallel, conditional, and retry execution but has no built-in way to apply a step to every element of a collection. Today callers hand-roll this loop outside the pipeline, losing all of the framework's validation, retry, structured logging, and metrics for each item. The pattern is frequent enough in real API pipelines (bulk enrichment, per-item downstream calls, batch transformation) that it belongs in the framework.

Mastra's `.foreach(step, { concurrency })` is the closest prior art and the reference design. FlowPipe diverges deliberately: strong Java types, fail-fast semantics on first item failure, and Lambda-safety constraints on thread management.

Existing engine types: `EngineNode` (sealed interface), `StepNode`, `ParallelNode`, `BranchNode`. The new `ForeachNode` joins this set. `Pipeline.executeShared` dispatches on node type via `instanceof`. `PipelineBuilder` holds the wiring logic and `currentOutputType` for build-time checks.

## Goals / Non-Goals

**Goals:**
- `PipelineBuilder.foreach(step)` and `foreach(step, concurrency)` methods that require the current pipeline output to be `List<E>` and advance it to `List<R>` where the step is `Step<E, R>`.
- Build-time type check: `currentOutputType` must be `List.class` at the point `foreach` is called; violations throw `PipelineBuildException`.
- Bounded concurrency: concurrency=1 runs items sequentially (no thread submission); concurrency>1 partitions the item list into windows and processes each window via the pipeline's `ExecutorService`.
- Per-item observability: step.start / step.finish / step.error log events and MetricsRecorder calls emitted per item, keyed `<stepId>[<index>]`.
- Per-item retry: `StepDescriptor.retryPolicy()` on the inner step is honoured independently per item, identical to how `StepNode` handles retry.
- Fail-fast: first item failure cancels in-flight futures (within the current window) and returns a `Failure` with the inner step's id.

**Non-Goals:**
- Partial success / collecting errors per item — failure is all-or-nothing, consistent with every other FlowPipe node type.
- Nested foreach (foreach inside foreach) — no restriction, falls naturally from the type system.
- Dynamic concurrency adjustment or back-pressure — not needed for synchronous Lambda-compatible use.
- Streaming or incremental result emission — no streaming model exists in FlowPipe.

## Decisions

### 1. Type erasure boundary: runtime check at `foreach()`, not at `build()`

Java erases `List<E>` to `List` at runtime, so `currentOutputType` for a `List<Order>` pipeline is `List.class`. The `foreach` overloads check `currentOutputType.equals(List.class)` immediately when called (not deferred to `build()`), throwing `PipelineBuildException` with a helpful message if violated.

This is consistent with how `branch()` checks arm types: eagerly, at the builder call, before `build()`.

**Alternative considered:** silent unchecked cast — rejected because it shifts a wiring mistake to a runtime ClassCastException mid-pipeline, which is exactly the failure mode FlowPipe exists to prevent.

### 2. Return type: `PipelineBuilder<I, List<R>>`

`foreach(Step<E, R> step)` returns `PipelineBuilder<I, List<R>>`. The `currentOutputType` is stored as `List.class` (raw, erased) using the same `@SuppressWarnings` pattern as `parallel2/3/4` (which store `Object.class`). This lets `.then(nextStep)` skip the type check for `Object.class` — but `List.class` is concrete, so the existing `.then()` check will correctly reject a step whose `inputType()` is not `List`.

### 3. Concurrency=1 is sequential — no thread submission

When `concurrency=1` (the default), items are processed in a tight for-loop with no `ExecutorService` interaction. This avoids thread-pool overhead on Lambda (cold start, small lists) and makes the default path dead simple.

### 4. Concurrency>1: window-based partitioning over the pipeline executor

Items are split into windows of size `concurrency`. Each window's items are submitted as `Callable`s to the pipeline's `ExecutorService`, futures collected in order, and results gathered before the next window starts. This approach:
- Reuses the already-wired executor (no per-call thread-pool creation/destruction)
- Limits concurrency exactly to the configured value
- Is Lambda-safe: all submitted tasks complete before the method returns
- Keeps code parallel with `executeParallel` (same Future-based collection pattern)

**Alternative considered:** semaphore-throttled submission of all items at once — rejected for complexity; window partitioning achieves the same bound with simpler code and the same tail-latency profile for uniform workloads.

### 5. Retry extracted into a shared private method

The retry loop in `executeShared` for `StepNode` is extracted to a private `executeItemWithRetry(step, input, itemLabel, ...)` method. Both `StepNode` and foreach item execution call this method. `itemLabel` is `stepId` for normal steps and `stepId + "[" + index + "]"` for foreach items. This avoids duplicating the retry/backoff/observability wiring and makes the foreach path consistent by construction.

### 6. TraceEntry id: `<stepId>[<index>]`

Each item produces a `TraceEntry` with id `<stepId>[<index>]` (0-based). This allows callers inspecting `ExecutionTrace` to identify which item failed. The format is unambiguous as step ids must not contain `[`.

### 7. ForeachNode is a new sealed type in `io.flowpipe.engine`

`ForeachNode<E, R>` is a package-private record implementing `EngineNode`. The `EngineNode` sealed interface's `permits` clause is extended. `PipelineBuilder.validate()` extends `collectIds` to register the inner step's id (duplicate step-id detection covers foreach steps).

## Risks / Trade-offs

- **Large lists with concurrency>1 hold all futures in memory** — not a problem for the target use case (API fan-out, typically tens to low-hundreds of items), but callers should be aware that `concurrency=N` submits a window of N tasks simultaneously. Mitigation: document the expected scale in the spec.
- **Executor saturation** — if the pipeline executor has fewer threads than the requested concurrency, tasks queue rather than run concurrently. This is not a bug but callers should size their executor accordingly. The `ForkJoinPool.commonPool()` default is fine for compute-bound work; I/O-bound workloads benefit from a dedicated pool.
- **Type erasure gap** — the compiler cannot verify that the pipeline's current list element type `E` matches the step's input type `E`. A mismatch causes `ValidationException` at runtime on first item execution. This is the same limitation as `parallel*` and is acceptable because the `StepDescriptor.inputType()` validator catches it on the first call.

## Migration Plan

Pure additive change. No existing API is modified, no behavior changes for existing pipelines. No migration needed. If a new dependency were required (it isn't), a deprecation cycle would apply — not relevant here.

## Open Questions

*(none — design is fully resolved)*
