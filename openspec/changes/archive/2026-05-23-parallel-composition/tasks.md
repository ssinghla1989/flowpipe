## 1. Engine internals refactor — introduce EngineNode

- [x] 1.1 Create `EngineNode<I, O>` as a package-private sealed interface in `io.flowpipe.engine` permitting `StepNode<I, O>` and `ParallelNode<I, O>`
- [x] 1.2 Create `StepNode<I, O>` as a package-private record wrapping a `Step<I, O>`
- [x] 1.3 Create `ParallelNode<I, O>` as a package-private record holding a `List<Step<I, ?>> branches` and a `Function<List<Object>, O> combiner`
- [x] 1.4 Refactor `PipelineBuilder` to store `List<EngineNode<?, ?>> nodes` instead of `List<Step<?, ?>> steps`; update `then()` to wrap its step in a `StepNode`; update `validate()` to iterate nodes and descend into `ParallelNode.branches()` for the duplicate-id check; update `build()` to pass the node list to `Pipeline`
- [x] 1.5 Refactor `Pipeline` to store and iterate `List<EngineNode<?, ?>> nodes`; update the execution loop to dispatch `StepNode` exactly as before (no observable change to sequential behavior) — parallel dispatch is a stub/placeholder that throws `UnsupportedOperationException` until task group 3
- [x] 1.6 Run `./gradlew test` and confirm all existing tests still pass (no regressions from the structural refactor)

## 2. Public API additions in `flowpipe-core`

- [x] 2.1 Define `TriFunction<A, B, C, R>` as a `@FunctionalInterface` in `io.flowpipe.api`
- [x] 2.2 Define `QuadFunction<A, B, C, D, R>` as a `@FunctionalInterface` in `io.flowpipe.api`
- [x] 2.3 Add `PipelineBuilder.withExecutor(ExecutorService executor)`: stores the executor (replaces previous; null throws NPE); returns `this`; default is `ForkJoinPool.commonPool()` set at `start()` time
- [x] 2.4 Add `PipelineBuilder.parallel2(BiFunction<A,B,R>, Step<O,A>, Step<O,B>)` returning `PipelineBuilder<I,R>`: creates a `ParallelNode`, advances the cursor type to `R`
- [x] 2.5 Add `PipelineBuilder.parallel3(TriFunction<A,B,C,R>, Step<O,A>, Step<O,B>, Step<O,C>)` returning `PipelineBuilder<I,R>`
- [x] 2.6 Add `PipelineBuilder.parallel4(QuadFunction<A,B,C,D,R>, Step<O,A>, Step<O,B>, Step<O,C>, Step<O,D>)` returning `PipelineBuilder<I,R>`
- [x] 2.7 Add `PipelineBuilder.parallelN(Class<R> resultType, Map<String, Step<O, ?>> steps, Function<Map<String,Object>, R> combiner)` returning `PipelineBuilder<I,R>`
- [x] 2.8 Forward the executor from `PipelineBuilder` through `build()` into `Pipeline`

## 3. Parallel execution in the engine

- [x] 3.1 Implement `ParallelNode` dispatch in `Pipeline.execute(...)`: for each branch, submit a `Callable` to the executor that validates input, calls `step.execute(...)`, validates output, records the `TraceEntry`, and emits per-branch observability (start/finish/error logs + recorder calls) using the same helper methods already used for sequential steps
- [x] 3.2 Collect `Future<BranchResult>` (where `BranchResult` carries the output value and `TraceEntry`) for each branch using a thread-safe list
- [x] 3.3 Join all futures with `Future.get()` (no timeout); on `ExecutionException` or `RejectedExecutionException`, unwrap the cause (strip `ExecutionException` wrapper), call `Future.cancel(true)` on all remaining futures, and return `Failure` identifying the failing branch's step id
- [x] 3.4 On full success, invoke the `ParallelNode.combiner` with branch results in declaration order (not completion order), then continue the pipeline with the combined value
- [x] 3.5 Accumulate branch `TraceEntry`s into the pipeline's `ExecutionTrace.Builder` (insertion order = completion order, which is non-deterministic — document this)

## 4. Build-time validation additions

- [x] 4.1 Add parallel-arity check in `validate()`: any `ParallelNode` with fewer than 2 branches throws `PipelineBuildException` with a message stating a minimum of 2 branches is required
- [x] 4.2 Extend the duplicate-id scan in `validate()` to descend into each `ParallelNode.branches()` alongside sequential `StepNode`s
- [x] 4.3 In `parallel2`–`parallel4` and `parallelN`, validate at call time that none of the supplied steps is `null` (throw NPE with a clear message); the arity check is deferred to `build()` (i.e., there's no arity check at `.parallelX()` call time since the compiler already enforces it)
- [x] 4.4 In `parallelN`, add a `build()`-time check that each map key equals the corresponding step's `StepDescriptor.id()`; mismatch throws `PipelineBuildException`

## 5. Spec-driven test coverage in `flowpipe-core`

- [x] 5.1 Parallel execution — two branches both receive the same input
- [x] 5.2 Combiner receives branch outputs in declaration order (not completion order)
- [x] 5.3 One failing branch produces `Failure` with that branch's step id; subsequent sequential steps are not invoked
- [x] 5.4 `Failure.cause()` is the branch's original exception, not a wrapping `ExecutionException`
- [x] 5.5 `parallel2` advances builder output type to combiner return type (compile-time; verified by the fact the code compiles and the result is typed correctly)
- [x] 5.6 Per-branch `step.start` and `step.finish` events are emitted (one per branch id) — use `Slf4jTestAppender`
- [x] 5.7 Failing branch produces `step.error`, not `step.finish`, for that branch's id
- [x] 5.8 Per-branch recorder receives duration + attempts + outcome for each branch
- [x] 5.9 Success trace contains entries for all branches
- [x] 5.10 Shut-down executor causes `Failure`, not an unchecked exception propagating from `execute()`
- [x] 5.11 `build()` rejects a `parallelN` block with one entry (arity < 2)
- [x] 5.12 `build()` rejects a step id duplicated across a sequential step and a parallel branch
- [x] 5.13 `build()` rejects a `parallelN` map key that disagrees with the step's descriptor id
- [x] 5.14 `withExecutor(null)` throws `NullPointerException`
- [x] 5.15 `parallelN` combiner receives a `Map` keyed by step ids with the correct output values

## 6. Documentation

- [x] 6.1 Update `CLAUDE.md` "Module and package layout" to note `EngineNode` / `StepNode` / `ParallelNode` are internal to `io.flowpipe.engine`, and list `TriFunction` / `QuadFunction` in `io.flowpipe.api`
- [x] 6.2 Update `README.md` to show a `parallel2` example alongside the existing sequential example
- [x] 6.3 Add a note in `README.md` about `ForkJoinPool.commonPool()` and the Lambda recommendation to supply a dedicated executor
- [x] 6.4 Verify the full build is green: `./gradlew clean build`
