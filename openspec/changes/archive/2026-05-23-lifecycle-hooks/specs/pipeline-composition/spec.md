## ADDED Requirements

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
