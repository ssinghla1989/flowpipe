# step-execution

## Purpose

Defines the `Step<I, O>` contract — what its single `execute` method receives (typed input plus a `StepContext` exposing `State` and `RequestContext`), what it must return, and how input/output validation and exception capture wrap every invocation so that step authors write no validation, error-handling, or context-plumbing boilerplate.

## Requirements

### Requirement: Step contract is a single-method interface with a descriptor

The library SHALL define `Step<I, O>` as an interface with exactly two methods: `StepDescriptor<I, O> describe()` and `O execute(I input, StepContext ctx) throws Exception`. Step authors SHALL NOT be required to implement any other methods. `StepDescriptor<I, O>` SHALL carry, at minimum, `String id()`, `Class<I> inputType()`, `Class<O> outputType()`, `Validator<I> inputValidator()`, and `Validator<O> outputValidator()`.

#### Scenario: Author implements only describe and execute

- **WHEN** a developer creates a step by implementing `Step<I, O>`
- **THEN** the compiler MUST require implementations of only `describe()` and `execute(I, StepContext)`

### Requirement: Engine validates step input before execute

For every step invocation, the engine SHALL call `descriptor.inputValidator().validate(input)` before calling `step.execute(input, ctx)`. A `ValidationException` thrown by the input validator SHALL terminate the pipeline immediately as a `Failure` whose `failedStepId` is the validating step's id, and `step.execute(...)` SHALL NOT be invoked.

#### Scenario: Invalid input prevents execute and yields Failure

- **WHEN** a pipeline executes a step whose input validator throws `ValidationException` for the supplied input
- **THEN** the step's `execute` method MUST NOT be invoked, the pipeline result MUST be a `Failure`, and `Failure.failedStepId()` MUST equal the step's id

### Requirement: Engine validates step output after execute

For every successful step invocation, the engine SHALL call `descriptor.outputValidator().validate(output)` after `step.execute(...)` returns. A `ValidationException` thrown by the output validator SHALL terminate the pipeline immediately as a `Failure` whose `failedStepId` is the validating step's id; no subsequent steps SHALL run.

#### Scenario: Invalid output halts the pipeline

- **WHEN** a step returns a value that its output validator rejects
- **THEN** the pipeline result MUST be a `Failure`, `Failure.failedStepId()` MUST equal that step's id, and any subsequent steps in the pipeline MUST NOT be invoked

### Requirement: Default validators are no-ops

`StepDescriptor` SHALL supply a `NoOpValidator` for both `inputValidator()` and `outputValidator()` whenever the step author does not provide a validator. The no-op validator SHALL accept any input including `null` without throwing.

#### Scenario: Step without validators runs unimpeded

- **WHEN** a developer constructs a step whose descriptor does not specify input or output validators and the engine invokes it
- **THEN** validation MUST pass and `execute` MUST be invoked normally with the supplied input

### Requirement: Step exceptions are captured as Failure

Any `Throwable` thrown by `step.execute(...)` SHALL be caught by the engine and surfaced as a `Failure` whose `cause()` is the thrown throwable and whose `failedStepId()` is the throwing step's id. Subsequent steps in the pipeline MUST NOT be invoked.

#### Scenario: Step throws and pipeline returns Failure

- **WHEN** a step's `execute` method throws a `RuntimeException`
- **THEN** the pipeline result MUST be a `Failure`, `Failure.cause()` MUST be the thrown exception, `Failure.failedStepId()` MUST equal the throwing step's id, and any subsequent steps MUST NOT be invoked

### Requirement: StepContext exposes State and RequestContext to execute

The `StepContext` passed to `step.execute(input, ctx)` SHALL expose two accessors: `State state()` returning the mutable execution-scoped state object, and `RequestContext context()` returning the immutable request-scoped context object. Both accessors SHALL return non-null instances even if the caller supplied no entries.

#### Scenario: execute receives non-null state and context

- **WHEN** the engine invokes a step's `execute(input, ctx)` for a pipeline started with an empty `RequestContext`
- **THEN** `ctx.state()` MUST return a non-null `State` and `ctx.context()` MUST return a non-null `RequestContext`
