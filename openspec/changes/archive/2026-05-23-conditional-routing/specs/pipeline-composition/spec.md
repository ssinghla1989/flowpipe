## ADDED Requirements

### Requirement: PipelineBuilder exposes a branch composition method

The builder SHALL expose a `branch(String branchId, BiPredicate<O, StepContext> predicate, Pipeline<O, R> ifTrue, Pipeline<O, R> ifFalse)` method that appends a conditional `BranchNode` to the pipeline under construction and returns a new `PipelineBuilder<I, R>`. This method SHALL follow the same consumed-state rules as `.then(...)`: calling it on a consumed builder SHALL throw `IllegalStateException`, and the returned builder is the active builder going forward. `java.util.function.BiPredicate` MUST be used; no new functional interface is introduced.

#### Scenario: branch is callable on an active builder and returns a new active builder

- **WHEN** a developer calls `.branch(...)` on a builder that is not yet consumed
- **THEN** the original builder MUST become consumed, and the returned builder MUST accept further `.then(...)` or `.build()` calls

#### Scenario: calling branch() on a consumed builder throws IllegalStateException

- **WHEN** a developer calls `.branch(...)` on a builder that was previously consumed by `.then(...)` or `.build()`
- **THEN** the call MUST throw `IllegalStateException`

## MODIFIED Requirements

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
