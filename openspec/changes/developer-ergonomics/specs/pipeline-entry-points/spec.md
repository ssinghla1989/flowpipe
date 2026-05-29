## ADDED Requirements

### Requirement: Pipeline.builder() is a discoverable alias for PipelineBuilder.start()
`Pipeline` SHALL provide a static method `builder(Class<I> inputType)` that returns a `PipelineBuilder<I, I>` — identical in behavior to `PipelineBuilder.start(inputType)`. Calling `Pipeline.builder(null)` SHALL throw `NullPointerException` with an appropriate message, consistent with `PipelineBuilder.start()`.

#### Scenario: Pipeline.builder() returns a working PipelineBuilder
- **WHEN** `Pipeline.builder(String.class).then(step).build()` is called
- **THEN** the resulting pipeline executes identically to one built via `PipelineBuilder.start(String.class).then(step).build()`

#### Scenario: Null input type is rejected
- **WHEN** `Pipeline.builder(null)` is called
- **THEN** `NullPointerException` is thrown

#### Scenario: Pipeline.builder() is callable without importing PipelineBuilder
- **WHEN** a developer uses only `import io.flowpipe.engine.Pipeline`
- **THEN** `Pipeline.builder(...)` compiles and provides the full builder API

### Requirement: PipelineBuilder.start() remains unchanged and undeprecated
`PipelineBuilder.start()` SHALL continue to work identically as before. It SHALL NOT be deprecated. Both entry points SHALL be equally supported.

#### Scenario: PipelineBuilder.start() still works after adding Pipeline.builder()
- **WHEN** existing code using `PipelineBuilder.start()` is compiled against the updated library
- **THEN** it compiles and behaves identically — no source or binary incompatibility
