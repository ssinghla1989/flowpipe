## ADDED Requirements

### Requirement: PipelineBuilder exposes typed parallel methods for arities 2 through 4

The builder SHALL expose `parallel2(BiFunction<A,B,R> combiner, Step<O,A> a, Step<O,B> b)`, `parallel3(TriFunction<A,B,C,R> combiner, Step<O,A> a, Step<O,B> b, Step<O,C> c)`, and `parallel4(QuadFunction<A,B,C,D,R> combiner, Step<O,A> a, Step<O,B> b, Step<O,C> c, Step<O,D> d)` methods, each returning a `PipelineBuilder<I, R>` whose current output type is `R` (the combiner's return type). `TriFunction` and `QuadFunction` SHALL be defined as `@FunctionalInterface` types in `io.flowpipe.api`.

#### Scenario: parallel2 advances the builder's output type to the combiner's return type

- **WHEN** a developer calls `.parallel2((a, b) -> a + b.length(), stepA, stepB)` where `stepA: Step<String, String>` and `stepB: Step<String, Integer>` and the combiner is `BiFunction<String, Integer, String>`
- **THEN** the resulting builder MUST have output type `String` and the expression MUST compile without casting

### Requirement: PipelineBuilder accepts an ExecutorService for parallel dispatch

The builder SHALL expose a `.withExecutor(ExecutorService executor)` method that returns the same (or a chainable) builder and stores the executor for use by all parallel blocks in the built pipeline. Passing `null` SHALL throw `NullPointerException`. If `.withExecutor(...)` is not called, the default executor SHALL be `ForkJoinPool.commonPool()`. Subsequent calls to `.withExecutor(...)` SHALL replace the previous value.

#### Scenario: default executor is ForkJoinPool.commonPool

- **WHEN** a pipeline with a parallel block is built without calling `.withExecutor(...)`
- **THEN** the built pipeline MUST execute branches using `ForkJoinPool.commonPool()` (observable by confirming the executing thread is a ForkJoinWorkerThread or the calling thread if pool parallelism is 0)

### Requirement: build() rejects a parallel block with fewer than 2 branches

If a `parallel2`–`parallel4` or `parallelN` call is somehow constructed with fewer than 2 step entries, `build()` SHALL throw `PipelineBuildException` with a message indicating that a parallel block requires at least 2 branches.

#### Scenario: parallelN with one entry fails build

- **WHEN** a developer calls `parallelN` with a map containing only one entry and calls `build()`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message indicates a minimum of 2 branches is required

### Requirement: build() rejects step ids duplicated across parallel branches and sequential steps

The existing duplicate-id check SHALL be extended to scan all steps in all parallel blocks in addition to sequential steps. A step id that appears in a parallel branch AND in a sequential step, or in two different parallel branches, SHALL cause `build()` to throw `PipelineBuildException` whose message lists the duplicated id(s).

#### Scenario: Id shared between a sequential step and a parallel branch is rejected

- **WHEN** a pipeline is built with a sequential step id `"validate"` and a parallel branch also using id `"validate"`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"validate"`
