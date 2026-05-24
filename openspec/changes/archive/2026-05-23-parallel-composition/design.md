## Context

FlowPipe currently executes steps in a strict linear chain. The engine's execution loop iterates a `List<Step<?,?>>`, executing each step and forwarding its output to the next. This slice adds fan-out/fan-in: given the same input, multiple steps run concurrently and their outputs are merged by a caller-supplied combiner before the pipeline continues.

Two constraints from `CLAUDE.md` are load-bearing here:

- **All per-step observability is automatic.** Each parallel branch must receive the same `step.start/finish/error` log events and `MetricsRecorder` calls as a sequential step. The engine, not the step, is responsible for this — so the engine must "see" each branch individually.
- **No background threads outliving a request.** The executor's lifecycle belongs entirely to the caller. FlowPipe's `execute()` must block until every branch completes before returning.

## Goals / Non-Goals

**Goals:**
- Typed parallel overloads for arities 2–4 (`parallel2` through `parallel4`) with compile-time-safe combiners.
- A `parallelN` escape hatch for higher arities (un-typed combiner, documented as last resort).
- `.withExecutor(ExecutorService)` on `PipelineBuilder`; default `ForkJoinPool.commonPool()`.
- Per-branch observability (logs + metrics) identical to sequential steps.
- Fail-fast on any branch failure — the parallel block returns `Failure` immediately.
- Build-time rejection of degenerate parallel blocks (arity < 2) and cross-pipeline duplicate step ids.

**Non-Goals:**
- Partial-failure semantics (some branches succeed, some fail — collect all results). All-or-nothing is sufficient for the synchronous API use-case.
- Per-block timeout. `Future.get()` without timeout is the baseline; revisit if a real use-case demands it.
- Branch cancellation on failure (best-effort; `Future.cancel(true)` is called but interrupt-responsiveness depends on the step).
- Ordered result streaming before all branches complete.
- Thread pool configuration beyond `withExecutor`.

## Decisions

### D1: Engine sees internal `EngineNode`s, not raw `Step`s

The current engine iterates `List<Step<?,?>>`. Changing the internal list to `List<EngineNode<?,?>>` where `EngineNode` is a sealed interface lets the execution loop dispatch to the right handler per node type without touching the user-facing `Step<I,O>` interface.

```java
// io.flowpipe.engine (package-private)
sealed interface EngineNode<I, O> permits StepNode, ParallelNode {}
record StepNode<I, O>(Step<I, O> step) implements EngineNode<I, O> {}
record ParallelNode<I, O>(
    List<Step<I, ?>> branches,
    java.util.function.Function<java.util.List<Object>, O> combiner
) implements EngineNode<I, O> {}
```

`PipelineBuilder.then()` wraps the step in a `StepNode`; `parallel2()`–`parallelN()` create a `ParallelNode` whose combiner is adapted from the typed user-facing function.

**Alternatives considered:** Wrapping each parallel block as a single `Step<I,O>` (rejected — the step contract's `execute(I, StepContext)` has no way to receive the recorder/logger needed for per-branch observability without changing the Step API). Pure reflection/map-based dispatch (rejected — loses type safety in the engine internals and makes the execution loop harder to reason about).

### D2: Typed combiners via small functional interfaces (`TriFunction`, `QuadFunction`)

Java stdlib provides `BiFunction<A,B,R>` for arity 2. For arities 3 and 4, FlowPipe defines:

```java
// io.flowpipe.api
@FunctionalInterface
public interface TriFunction<A, B, C, R> { R apply(A a, B b, C c); }

@FunctionalInterface
public interface QuadFunction<A, B, C, D, R> { R apply(A a, B b, C c, D d); }
```

These are user-facing (the developer passes a lambda as the combiner), so they belong in `io.flowpipe.api`. The `ParallelNode` stores a `Function<List<Object>, O>` internally; the builder adapts each typed combiner into this form, casting the positional elements.

**Alternatives considered:** Always using `Function<Map<String, Object>, R>` even for typed arities (rejected — forces the developer to write a map-lookup combiner and defeats the type-safe goal). A `ParallelResult<A,B>` record hierarchy per arity (rejected — excessive API surface, worse ergonomics than a simple lambda).

### D3: Fail-fast on any branch failure — cancel remaining, return `Failure`

When any branch's `Future.get()` throws `ExecutionException` or `CancellationException`, the engine immediately calls `Future.cancel(true)` on all other in-flight futures, collects any already-completed trace entries, and returns `Failure` with the originating step's id. This mirrors the sequential step behavior ("any thrown `Throwable` terminates the pipeline").

The `Failure.failedStepId()` is the id of the failing branch's step. If two branches fail simultaneously (in the window before the join loop processes them), the first failure encountered in join order wins.

**Alternatives considered:** Waiting for all branches to complete even if one fails, collecting multiple errors (rejected — more complex, inconsistent with sequential failure semantics, and the use case of partial-parallel-failure recovery is better modeled with explicit error-returning steps).

### D4: `withExecutor(ExecutorService)` is a builder-level concern, forwarded to Pipeline

Like `withMetrics`, the executor is set once at build time. `.withExecutor(exec)` is chainable, replaces any prior value, null throws NPE. If the pipeline has no `ParallelNode`s, the stored executor is never used. Default: `ForkJoinPool.commonPool()`.

The pipeline never shuts down or monitors the executor. Submitting to a shutdown executor surfaces as `RejectedExecutionException`, which is caught and surfaced as a `Failure` (the branch's `execute()` effectively threw).

**Alternatives considered:** Passing the executor at `execute()` time (rejected — inconsistent with how `MetricsRecorder` is handled; build-time default with per-call override could be added later). Creating a dedicated pool per pipeline (rejected — background resource, breaks the no-daemon constraint for Lambda).

### D5: Per-branch observability identical to sequential steps

The parallel execution handler in the engine calls the same `emitStart()` / `emitFinish()` / `emitError()` and `emitRecord()` private methods as the sequential handler — once per branch, from the branch's worker thread. The `MetricsRecorder` and SLF4J `Logger` are passed into the worker callable by closure. Because SLF4J logging is thread-safe and MetricsRecorder is expected to be thread-safe (documented), this is correct.

`TraceEntry`s from all branches are collected in a thread-safe list and merged into the builder in the order branches complete (which is non-deterministic). The trace is therefore unordered within a parallel block, which is expected and documented.

### D6: `parallelN` is explicitly un-typed and documented as a last resort

```java
public <R> PipelineBuilder<I, R> parallelN(
    Class<R> resultType,
    java.util.Map<String, Step<O, ?>> steps,
    java.util.function.Function<java.util.Map<String, Object>, R> combiner
)
```

Steps are keyed by their desired id (which MUST match the step's `StepDescriptor.id()` — if they differ `build()` fails with a mismatch error). The combiner receives a `Map<String, Object>` keyed by step id. The `resultType` token allows the builder to advance its cursor class.

`parallelN` is the valve that covers any arity above 4. Its ergonomics are intentionally worse than `parallel2`–`parallel4` as a design signal.

### D7: `build()` new validation rules

Two new checks added alongside existing ones:

1. **Parallel block empty/singleton**: A `ParallelNode` with 0 or 1 branches throws `PipelineBuildException`. The message names the parallel block's position and suggests using `.then()` for a single step.
2. **Cross-node duplicate ids**: The existing duplicate-id scan now descends into `ParallelNode.branches()` as well as `StepNode`s. A step in a parallel branch that shares an id with a sequential step (or another branch) is rejected.

`parallelN` also validates that each map key matches the corresponding step's `StepDescriptor.id()`.

## Risks / Trade-offs

- **`ForkJoinPool.commonPool()` is shared process-wide.** If a step blocks on I/O in a parallel branch, it holds a common-pool thread. The common pool's default parallelism equals `Runtime.availableProcessors() - 1`. On Lambda (1 vCPU), that is 0 — the common pool degrades to the calling thread, making parallelism illusory. → **Mitigation**: document prominently; advise Lambda users to supply a dedicated `Executors.newCachedThreadPool()` executor. The default is convenient for development, not optimal for Lambda with blocking I/O.
- **Non-deterministic trace order within a parallel block.** Downstream steps that inspect `ExecutionTrace` by index will see non-deterministic ordering. → Mitigation: documented; `ExecutionTrace` iteration should not assume ordering within a parallel block.
- **Introducing `EngineNode` breaks the current `List<Step<?,?>>` field** in `Pipeline` and `PipelineBuilder`. This is an internal-only change (both are package-private in construction), but it touches most of the engine. → Mitigation: all existing tests continue to pass; the internal refactor is covered by the existing test suite before new tests are added.
- **`TriFunction` and `QuadFunction` in `io.flowpipe.api` become permanent API** once shipped. → Mitigation: they are simple `@FunctionalInterface` types with a single `apply` method. The risk of regret is very low.

## Migration Plan

Strictly additive. All existing `PipelineBuilder.start(...).then(...).build()` chains and all `Pipeline.execute(...)` callsites work unchanged. No existing API is modified.
