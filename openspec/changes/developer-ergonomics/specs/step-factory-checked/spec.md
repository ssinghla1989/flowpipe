## ADDED Requirements

### Requirement: Step.ofChecked factory accepts checked-exception bodies
`Step` SHALL provide a static factory method `ofChecked(String id, Class<I> inputType, Class<O> outputType, CheckedBiFunction<I, StepContext, O> body)` that creates a `Step<I, O>` whose `execute()` delegates to `body`. The body MAY throw any checked or unchecked exception; exceptions propagate directly out of `execute()` without wrapping. The returned step's `StepDescriptor` SHALL have no validators, no retry policy, no timeout policy, and no circuit-breaker policy by default.

#### Scenario: Lambda body with checked exception compiles without wrapping
- **WHEN** a developer writes `Step.ofChecked("id", In.class, Out.class, (in, ctx) -> service.call(in))` where `service.call()` throws `IOException`
- **THEN** the expression compiles without requiring a try/catch or `RuntimeException` wrapping around the checked exception

#### Scenario: Checked exception from body surfaces as Failure
- **WHEN** a step created via `ofChecked` is wired into a pipeline, and its body throws a checked exception during execution
- **THEN** the pipeline returns a `Failure` whose `cause()` is the original checked exception (not wrapped in `RuntimeException`)

#### Scenario: Successful execution returns Success
- **WHEN** a step created via `ofChecked` is wired into a pipeline and its body returns a non-null value
- **THEN** the pipeline returns a `Success` with that value

#### Scenario: Policy decoration still works
- **WHEN** a step returned by `ofChecked` has `.withRetry(policy)` or `.withTimeout(policy)` chained onto it
- **THEN** the resulting step behaves identically to a policy-decorated step created via `Step.of()`

### Requirement: CheckedBiFunction is a public functional interface
A `@FunctionalInterface` named `CheckedBiFunction<I, C, O>` SHALL exist in `io.flowpipe.api` with a single abstract method `O apply(I input, C ctx) throws Exception`. This interface MAY be used directly by callers who hold a reference to a checked body.

#### Scenario: Interface is annotated as FunctionalInterface
- **WHEN** a developer references `CheckedBiFunction` in a lambda expression
- **THEN** the lambda is accepted without cast, the same as any other `@FunctionalInterface`
