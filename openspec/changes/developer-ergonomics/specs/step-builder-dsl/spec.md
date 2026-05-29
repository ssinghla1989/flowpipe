## ADDED Requirements

### Requirement: Step.builder() returns a StepBuilder with fluent policy configuration
`Step` SHALL provide a static method `builder(String id, Class<I> inputType, Class<O> outputType)` returning a `StepBuilder<I, O>`. The `StepBuilder` SHALL support the following fluent methods in any order: `.execute(CheckedBiFunction<I, StepContext, O>)` (required before `.build()`), `.withRetry(RetryPolicy)`, `.withTimeout(TimeoutPolicy)`, `.withCircuitBreaker(CircuitBreakerPolicy)`, `.withOutputKey(StateKey<O>)`, `.withInputValidator(Validator<I>)`, `.withOutputValidator(Validator<O>)`. Calling `.build()` without first calling `.execute()` SHALL throw `IllegalStateException`.

#### Scenario: Builder produces a step with all configured policies
- **WHEN** a developer calls `Step.builder("id", In.class, Out.class).execute(body).withRetry(policy).withTimeout(tp).build()`
- **THEN** the returned step's `describe()` returns a `StepDescriptor` with `id="id"`, the configured `RetryPolicy`, and the configured `TimeoutPolicy`

#### Scenario: Builder without execute() throws on build
- **WHEN** `.build()` is called on a `StepBuilder` that has not had `.execute()` called
- **THEN** `IllegalStateException` is thrown with a message indicating the body is required

#### Scenario: Builder body may throw checked exceptions
- **WHEN** the body passed to `.execute()` throws a checked exception during pipeline execution
- **THEN** the pipeline returns a `Failure` whose `cause()` is the original checked exception, not wrapped

#### Scenario: Builder with no policies uses defaults
- **WHEN** `Step.builder("id", In.class, Out.class).execute(body).build()` is called with no policy configuration
- **THEN** the step has `RetryPolicy.none()`, `TimeoutPolicy.none()`, and no circuit-breaker — identical defaults to `Step.of()`

#### Scenario: Builder policies are identical to chained withX() calls
- **WHEN** a step is built via `Step.builder(...).execute(body).withRetry(p).build()`
- **THEN** executing that step in a pipeline produces the same retry behavior as `Step.of(..., body).withRetry(p)`

### Requirement: StepBuilder is in the public API package
`StepBuilder<I, O>` SHALL be a public type in `io.flowpipe.api`, usable without referencing internal packages. Developers MAY hold a `StepBuilder` reference and configure it before calling `.build()`.

#### Scenario: StepBuilder is referenceable in user code
- **WHEN** a developer declares a variable `StepBuilder<String, Integer> builder = Step.builder(...)`
- **THEN** the code compiles without importing anything from `io.flowpipe.engine`
