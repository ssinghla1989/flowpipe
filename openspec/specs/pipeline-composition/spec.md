# pipeline-composition

## Purpose

Defines how a developer composes typed `Step`s into an immutable, executable `Pipeline` via a fluent `PipelineBuilder`, and the build-time guarantees the builder makes — type-chain integrity across `.then(...)` calls, rejection of empty pipelines, and rejection of duplicate step ids.

## Requirements

### Requirement: Fluent type-tracking pipeline builder

The library SHALL provide a `PipelineBuilder<I, O>` whose generic parameters track the pipeline's input type and current output type. A `.then(Step<O, X>)` call SHALL return a `PipelineBuilder<I, X>`. Wiring a step whose declared input type does not match the builder's current output type SHALL be a compile-time error.

#### Scenario: Chained steps with compatible types compile

- **WHEN** a developer chains `PipelineBuilder.start(String.class).then(stepA).then(stepB)` where `stepA` is a `Step<String, Integer>` and `stepB` is a `Step<Integer, User>`
- **THEN** the expression compiles and the resulting builder's type is `PipelineBuilder<String, User>`

#### Scenario: Chained steps with incompatible types fail to compile

- **WHEN** a developer chains `PipelineBuilder.start(String.class).then(stepA).then(stepB)` where `stepA` is a `Step<String, Integer>` and `stepB` is a `Step<Boolean, User>`
- **THEN** the Java compiler MUST reject the second `.then(stepB)` call with a type-mismatch error

### Requirement: Pipeline entry point declares the input type explicitly

The builder SHALL be obtained via `PipelineBuilder.start(Class<I> inputType)`. The `inputType` class token SHALL be retained on the built `Pipeline<I, O>` and exposed via `pipeline.inputType()`.

#### Scenario: Pipeline retains its declared input type

- **WHEN** a developer calls `PipelineBuilder.start(Order.class).then(step1).build()`
- **THEN** the resulting `Pipeline.inputType()` returns `Order.class`

### Requirement: build() produces an immutable Pipeline

`PipelineBuilder.build()` SHALL return a new `Pipeline<I, O>` instance whose composed step list cannot be mutated after construction. Subsequent calls to any method on the consumed builder (including `.then()` and `.build()`) SHALL throw `IllegalStateException`.

#### Scenario: Calling then() after build() throws

- **WHEN** a developer calls `build()` on a builder and then calls `.then(...)` on the same builder reference
- **THEN** the second call MUST throw `IllegalStateException`

#### Scenario: Calling build() twice throws

- **WHEN** a developer calls `build()` twice on the same builder
- **THEN** the second call MUST throw `IllegalStateException`

### Requirement: build() rejects empty pipelines

If no `.then(...)` calls have been made, `build()` SHALL throw `PipelineBuildException` with a message identifying the pipeline as empty.

#### Scenario: Empty builder fails build

- **WHEN** a developer calls `PipelineBuilder.start(String.class).build()` with no `.then(...)` calls
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains the phrase "empty pipeline"

### Requirement: build() rejects duplicate step ids

If two or more steps registered via `.then(...)` share the same `StepDescriptor.id()`, `build()` SHALL throw `PipelineBuildException` whose message lists the duplicated id(s). This check SHALL be extended to include branch ids: a branch id that duplicates any step id (sequential, parallel, or within an arm sub-pipeline), or any other branch id in the same top-level pipeline, SHALL also cause `build()` to throw `PipelineBuildException` whose message contains the duplicated id.

#### Scenario: Duplicate step ids fail build

- **WHEN** a developer chains two steps whose descriptors return the same id (e.g., both return `"normalize"`) and calls `build()`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains the duplicated id `"normalize"`

#### Scenario: Branch id duplicating a sequential step id fails build

- **WHEN** a pipeline has a sequential step with id `"validate"` and a branch node with `branchId = "validate"`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"validate"`

#### Scenario: Two branch nodes with the same id fail build

- **WHEN** a pipeline has two `branch(...)` calls both using `branchId = "route"`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains `"route"`

### Requirement: then() rejects step-to-step type mismatches

When `.then(step)` is called, the supplied step's declared `inputType` (from its `StepDescriptor`) MUST equal the builder's current output type. If not, `.then(...)` SHALL throw `PipelineBuildException` whose message names the offending step id along with the expected and actual type names. This rule guards against generic-erasure escapes such as raw-typed step instances: a step that lies about its declared output type via raw coercion will be caught at the next `.then(...)` call, when the following step's declared input type does not match the previously claimed output.

#### Scenario: Next step's declared input type disagrees with previous output

- **WHEN** a developer chains a raw-coerced step whose `StepDescriptor.outputType()` returns `Integer.class` followed by a `Step<String, String>` whose descriptor declares `inputType` as `String.class`
- **THEN** the second `.then(...)` call MUST throw `PipelineBuildException` whose message names the offending step id, mentions `String`, and mentions `Integer`

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

### Requirement: PipelineBuilder exposes a branch composition method

The builder SHALL expose a `branch(String branchId, BiPredicate<O, StepContext> predicate, Pipeline<O, R> ifTrue, Pipeline<O, R> ifFalse)` method that appends a conditional `BranchNode` to the pipeline under construction and returns a new `PipelineBuilder<I, R>`. This method SHALL follow the same consumed-state rules as `.then(...)`: calling it on a consumed builder SHALL throw `IllegalStateException`, and the returned builder is the active builder going forward. `java.util.function.BiPredicate` MUST be used; no new functional interface is introduced.

#### Scenario: branch is callable on an active builder and returns a new active builder

- **WHEN** a developer calls `.branch(...)` on a builder that is not yet consumed
- **THEN** the original builder MUST become consumed, and the returned builder MUST accept further `.then(...)` or `.build()` calls

#### Scenario: calling branch() on a consumed builder throws IllegalStateException

- **WHEN** a developer calls `.branch(...)` on a builder that was previously consumed by `.then(...)` or `.build()`
- **THEN** the call MUST throw `IllegalStateException`

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

### Requirement: PipelineBuilder exposes a withLifecycle method for registering a PipelineLifecycle

The builder SHALL expose a `withLifecycle(PipelineLifecycle<I, O> lifecycle)` method that stores the lifecycle instance and returns the same (or a chainable) `PipelineBuilder<I, O>`. Passing `null` SHALL throw `NullPointerException`. If `.withLifecycle(...)` is not called, the pipeline SHALL use a no-op `PipelineLifecycle` instance. Subsequent calls to `.withLifecycle(...)` SHALL replace the previously registered instance. Calling `.withLifecycle(...)` on a consumed builder SHALL throw `IllegalStateException`.

#### Scenario: withLifecycle is callable on an active builder and the built pipeline uses it

- **WHEN** a developer calls `.withLifecycle(myLifecycle)` before `.build()` on an active builder
- **THEN** `build()` MUST succeed and the resulting pipeline MUST invoke `myLifecycle.onStart(...)` when executed

#### Scenario: withLifecycle null argument throws NullPointerException

- **WHEN** a developer calls `.withLifecycle(null)` on an active builder
- **THEN** the call MUST throw `NullPointerException`

#### Scenario: withLifecycle on a consumed builder throws IllegalStateException

- **WHEN** a developer calls `.withLifecycle(myLifecycle)` on a builder that was already consumed by a previous `.then(...)` call
- **THEN** the call MUST throw `IllegalStateException`

#### Scenario: Pipeline built without withLifecycle uses a no-op lifecycle

- **WHEN** a pipeline is built without calling `.withLifecycle(...)`
- **THEN** the pipeline MUST execute without error and no lifecycle callbacks MUST be invoked on any user-supplied object
