## Why

Sequential `.then()` composition covers only the trivial case where each downstream call must wait for the previous one. Real REST handlers routinely fan out to two or more independent services simultaneously (inventory check + pricing + shipping estimate, etc.) and the current engine forces those calls to be serialised, adding latency the user pays for on every request. Parallel composition removes that cost and completes the "sequential, parallel, and conditional" promise that defines FlowPipe's value proposition.

## What Changes

- Introduce `.parallel2()` through `.parallel4()` overloads on `PipelineBuilder` that accept a typed combiner function and 2–4 steps. All steps receive the same input concurrently; the combiner merges their typed outputs into a single value; the builder returns a new `PipelineBuilder` whose current output type is the combiner's return type. Type safety is enforced at compile time via the combiner's function type.
- Introduce `.parallelN()` as a variadic escape hatch accepting a `Map<String, Step<I, ?>>` plus a `Function<Map<String, Object>, R>` combiner. This is explicitly un-typed and documented as a last resort for arities above 4.
- The parallel block is a **synchronization point**: all branches must complete before the pipeline continues. If any branch throws or fails validation, the entire parallel block fails immediately (the remaining branches' results are discarded) and the pipeline returns a `Failure` identifying the failing step.
- Parallel step execution uses a `java.util.concurrent.ExecutorService` injected at build time via `PipelineBuilder.withExecutor(ExecutorService)`. Default: `ForkJoinPool.commonPool()`. The executor's lifecycle is wholly owned by the caller — FlowPipe never shuts it down.
- All per-step observability (SLF4J `step.start/finish/error` and `MetricsRecorder` emission) applies to each parallel branch exactly as it does to sequential steps.
- `build()` gains two new validation rules: (a) reject a parallel block with 0 or 1 steps (use `.then()` instead); (b) reject a parallel block where any step id duplicates another step id already in the pipeline (existing duplicate-id rule now spans parallel branches).
- **No breaking changes** to existing `Pipeline.execute(...)` overloads or the `PipelineBuilder.then(...)` API.

## Capabilities

### New Capabilities
- `parallel-step-execution`: Defines how parallel blocks execute — concurrent branch dispatch, synchronization-point semantics, fail-fast behavior on branch failure, observability around each branch, executor lifecycle contract, and the `parallelN` escape hatch.

### Modified Capabilities
- `pipeline-composition`: The builder gains `.parallel2()` through `.parallel4()` and `.parallelN()` methods plus a `.withExecutor(ExecutorService)` method; `build()` gains two new rejection rules (parallel arity bounds and cross-branch duplicate id detection).

## Impact

- **`flowpipe-core` source**: `PipelineBuilder` gains `withExecutor`, `parallel2`–`parallel4`, `parallelN`. The engine adds a `ParallelNode` internal type that dispatches branches, joins results, and applies observability. `Pipeline` carries the executor reference from build time.
- **`flowpipe-test`**: No new test utilities needed; `StepHarness` only tests single steps, unchanged.
- **Dependencies**: No new runtime dependencies. `java.util.concurrent` is part of the JDK. `ForkJoinPool.commonPool()` is available on Java 17 Lambda runtimes.
- **Lambda compatibility**: The executor is injected, never created internally by FlowPipe. If a Lambda consumer passes `Executors.newCachedThreadPool()` they are responsible for its lifecycle. With the default `commonPool()`, no background threads outlive the request because `Future.get()` joins the work before `execute()` returns.
- **Downstream changes unblocked**: Branch composition (Phase 3b) and retry (Phase 4) can proceed once the parallel engine node pattern is established.
