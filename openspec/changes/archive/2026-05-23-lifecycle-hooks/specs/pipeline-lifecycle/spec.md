## ADDED Requirements

### Requirement: PipelineLifecycle is a default-method interface with three callback points

The library SHALL define `PipelineLifecycle<I, O>` as a public interface in `io.flowpipe.api` with three methods, each carrying a `default` no-op body: `onStart(I input, StepContext ctx)`, `onFinish(Result<O> result, StepContext ctx)`, and `onError(Failure<O> failure, StepContext ctx)`. Implementors SHALL be required to override only the callbacks they need; a class implementing `PipelineLifecycle` with no overrides MUST compile and run without error.

#### Scenario: Implementor overrides only onStart

- **WHEN** a developer implements `PipelineLifecycle<String, Integer>` and overrides only `onStart`
- **THEN** the class MUST compile and the unoverridden `onFinish` and `onError` callbacks MUST execute as no-ops when the pipeline runs

#### Scenario: Default no-op implementation compiles and runs

- **WHEN** a developer creates an anonymous `PipelineLifecycle<String, Integer>` implementation with no method overrides (using `new PipelineLifecycle<>() {}`)
- **THEN** the expression MUST compile and all three callbacks MUST be callable without throwing

### Requirement: onStart fires before any step executes and receives the pipeline input

The engine SHALL invoke `lifecycle.onStart(input, ctx)` before any pipeline node (step, parallel, or branch) is executed, using the same `StepContext` instance (and thus the same `State` and `RequestContext`) that will be passed to all steps. `onStart` MUST be called exactly once per top-level `pipeline.execute(...)` invocation; it MUST NOT be called for branch-arm sub-pipelines or parallel branches.

#### Scenario: onStart fires before the first step

- **WHEN** a pipeline with a registered lifecycle executes and `onStart` records a flag in `State`
- **THEN** the first step's `execute` method MUST observe that flag already set in `ctx.state()` when it runs

#### Scenario: onStart is not called for branch arms

- **WHEN** a pipeline contains a `branch(...)` node and a lifecycle is registered on the outer pipeline
- **THEN** `onStart` MUST be called exactly once (for the outer pipeline) and MUST NOT be called again when the chosen branch arm executes

### Requirement: onFinish fires after pipeline execution regardless of outcome

The engine SHALL invoke `lifecycle.onFinish(result, ctx)` after pipeline execution completes, whether the result is `Success` or `Failure`. `onFinish` MUST receive the same `Result<O>` instance that will be returned to the caller and the same `StepContext` used during execution. `onFinish` MUST be called exactly once per top-level execution.

#### Scenario: onFinish receives Success result on successful pipeline

- **WHEN** all steps succeed and a lifecycle is registered
- **THEN** `onFinish` MUST be called with a `Success<O>` result whose `value()` equals the pipeline output

#### Scenario: onFinish receives Failure result when a step throws

- **WHEN** a step throws and the pipeline returns a `Failure` and a lifecycle is registered
- **THEN** `onFinish` MUST be called with the `Failure<O>` result before the result is returned to the caller

### Requirement: onError fires after onFinish when the pipeline result is a Failure

The engine SHALL invoke `lifecycle.onError(failure, ctx)` immediately after `onFinish` when and only when the pipeline result is a `Failure`. `onError` MUST NOT be called on successful executions. The `Failure` passed to `onError` MUST be the same instance passed to `onFinish`.

#### Scenario: onError is called on failure after onFinish

- **WHEN** a step throws and a lifecycle tracks invocation order
- **THEN** `onFinish` MUST be called before `onError`, and `onError` MUST receive the same `Failure` instance

#### Scenario: onError is not called on success

- **WHEN** all steps succeed and a lifecycle is registered
- **THEN** `onError` MUST NOT be called

### Requirement: onStart exceptions terminate the pipeline as a Failure with stepId "pipeline.onStart"

If `onStart(input, ctx)` throws any `Throwable`, the engine SHALL catch it, halt execution immediately without running any step, and return a `Failure` whose `cause()` is the thrown throwable and whose `failedStepId()` is the string `"pipeline.onStart"`. `onFinish` and `onError` MUST NOT be called when `onStart` throws.

#### Scenario: onStart exception produces Failure without running steps

- **WHEN** `onStart` throws a `RuntimeException` and the pipeline has one step
- **THEN** the pipeline result MUST be a `Failure`, `Failure.cause()` MUST be the thrown exception, `Failure.failedStepId()` MUST equal `"pipeline.onStart"`, and the step's `execute` MUST NOT be invoked

#### Scenario: onStart exception suppresses onFinish and onError

- **WHEN** `onStart` throws and a lifecycle tracks invocation order
- **THEN** `onFinish` MUST NOT be called and `onError` MUST NOT be called

### Requirement: onFinish and onError exceptions are isolated and do not alter the pipeline result

If `onFinish(result, ctx)` or `onError(failure, ctx)` throws any `Throwable`, the engine SHALL catch it, log a warning (at the same severity as `metrics.recorder_failed`), and return the pipeline's original result unchanged. A throwing `onFinish` MUST NOT prevent `onError` from being called when the result is a `Failure`.

#### Scenario: onFinish exception does not change a Success result

- **WHEN** all steps succeed, `onFinish` throws a `RuntimeException`, and a lifecycle is registered
- **THEN** the pipeline result MUST still be the `Success` value, and the thrown exception MUST NOT be propagated to the caller

#### Scenario: onFinish exception does not suppress onError

- **WHEN** a step fails and `onFinish` throws, but `onError` is well-behaved
- **THEN** `onError` MUST still be called with the original `Failure`

#### Scenario: onError exception does not change a Failure result

- **WHEN** a step fails and `onError` throws a `RuntimeException`
- **THEN** the pipeline result MUST still be the original `Failure`, and the thrown exception MUST NOT be propagated to the caller

### Requirement: RecordingPipelineLifecycle in flowpipe-test captures invocations for assertion

`flowpipe-test` SHALL provide a `RecordingPipelineLifecycle<I, O>` class that implements `PipelineLifecycle<I, O>` and records each invocation in order, capturing the arguments supplied to each callback. Test code SHALL be able to query the recorded invocations to assert that hooks fired with the expected arguments.

#### Scenario: RecordingPipelineLifecycle captures onStart input

- **WHEN** a pipeline with a `RecordingPipelineLifecycle` is executed with a known input
- **THEN** the recorder's `onStartInvocations()` list MUST contain one entry whose input equals the supplied pipeline input

#### Scenario: RecordingPipelineLifecycle captures onError failure

- **WHEN** a step throws a known exception and a `RecordingPipelineLifecycle` is registered
- **THEN** the recorder's `onErrorInvocations()` list MUST contain one entry whose `failure.cause()` is the thrown exception
