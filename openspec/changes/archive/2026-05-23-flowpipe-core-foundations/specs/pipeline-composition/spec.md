## ADDED Requirements

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

If two or more steps registered via `.then(...)` share the same `StepDescriptor.id()`, `build()` SHALL throw `PipelineBuildException` whose message lists the duplicated id(s).

#### Scenario: Duplicate ids fail build

- **WHEN** a developer chains two steps whose descriptors return the same id (e.g., both return `"normalize"`) and calls `build()`
- **THEN** `build()` MUST throw `PipelineBuildException` whose message contains the duplicated id `"normalize"`

### Requirement: then() rejects step-to-step type mismatches

When `.then(step)` is called, the supplied step's declared `inputType` (from its `StepDescriptor`) MUST equal the builder's current output type. If not, `.then(...)` SHALL throw `PipelineBuildException` whose message names the offending step id along with the expected and actual type names. This rule guards against generic-erasure escapes such as raw-typed step instances: a step that lies about its declared output type via raw coercion will be caught at the next `.then(...)` call, when the following step's declared input type does not match the previously claimed output.

#### Scenario: Next step's declared input type disagrees with previous output

- **WHEN** a developer chains a raw-coerced step whose `StepDescriptor.outputType()` returns `Integer.class` followed by a `Step<String, String>` whose descriptor declares `inputType` as `String.class`
- **THEN** the second `.then(...)` call MUST throw `PipelineBuildException` whose message names the offending step id, mentions `String`, and mentions `Integer`
