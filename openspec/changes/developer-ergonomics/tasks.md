## 1. CheckedBiFunction interface and Step.ofChecked()

- [ ] 1.1 Add `CheckedBiFunction<I, C, O>` `@FunctionalInterface` to `io.flowpipe.api` with `O apply(I input, C ctx) throws Exception`
- [ ] 1.2 Add `Step.ofChecked(String id, Class<I> inputType, Class<O> outputType, CheckedBiFunction<I, StepContext, O> body)` static factory to `Step`
- [ ] 1.3 Write unit tests covering: checked exception propagates as Failure cause (not wrapped), successful execution returns Success, policy decoration still works

## 2. Result fluent API

- [ ] 2.1 Add `map(Function<O, X> fn)` default method to `Result<O>` sealed interface — Success delegates to fn, Failure re-types and returns self
- [ ] 2.2 Add `flatMap(Function<O, Result<X>> fn)` default method — Success applies fn and returns result, Failure re-types and returns self
- [ ] 2.3 Add `fold(Function<O, X> onSuccess, Function<Failure<O>, X> onFailure)` default method
- [ ] 2.4 Add `getOrElse(O defaultValue)` and `getOrElse(Supplier<O> supplier)` default methods
- [ ] 2.5 Add `getOrThrow()` (throws `RuntimeException(cause)`) and `getOrThrow(Function<Failure<O>, E> exMapper) throws E` default methods
- [ ] 2.6 Write unit tests for all methods covering both Success and Failure branches, including lazy supplier not evaluated on Success

## 3. Step.builder() DSL

- [ ] 3.1 Create `StepBuilder<I, O>` public class in `io.flowpipe.api` with fields for id, inputType, outputType, body, and all policy/validator fields
- [ ] 3.2 Add `execute(CheckedBiFunction<I, StepContext, O> body)` method — stores body, returns `this`
- [ ] 3.3 Add fluent `.withRetry()`, `.withTimeout()`, `.withCircuitBreaker()`, `.withOutputKey()`, `.withInputValidator()`, `.withOutputValidator()` methods, each returning `this`
- [ ] 3.4 Add `.build()` method — throws `IllegalStateException` if body not set; returns a `Step<I, O>` backed by the descriptor and body
- [ ] 3.5 Add `Step.builder(String id, Class<I> inputType, Class<O> outputType)` static factory on `Step` returning `StepBuilder<I, O>`
- [ ] 3.6 Write unit tests: build without execute() throws, policies on built step match configured policies, checked exception body propagates correctly, output matches Step.of() behavior for same body and policies

## 4. PipelineBuilder.when() shorthand

- [ ] 4.1 Add `when(String id, BiPredicate<O, StepContext> predicate, Step<O, O> step)` to `PipelineBuilder<I, O>` — internally wraps `step` in a single-step `Pipeline<O, O>` arm and delegates to the existing `branch(id, predicate, Pipeline<O, O>)` overload
- [ ] 4.2 Write unit tests covering: predicate true executes step, predicate false passes through, output type unchanged, skipped trace entry when false, `failedStepId` is branch id on predicate throw, `failedStepId` is step id on step throw, duplicate id detected at build time, blank id rejected immediately

## 5. Pipeline.builder() alias

- [ ] 5.1 Add `Pipeline.builder(Class<I> inputType)` static method on `Pipeline` — delegates to `PipelineBuilder.start(inputType)`, rejects null input
- [ ] 5.2 Write a smoke test confirming `Pipeline.builder(String.class).then(step).build()` produces an equivalent pipeline to `PipelineBuilder.start(String.class).then(step).build()`

## 6. Fix Steps.noop in flowpipe-test

- [ ] 6.1 Update `Steps.noop(String id)` in `flowpipe-test` to use a non-Void type (e.g., `String`) with an empty string body, or remove it and replace with a cleaner test utility that works inside real pipelines
- [ ] 6.2 Verify no existing test uses the broken `noop()` in a real pipeline context; update any such tests

## 7. Integration and docs

- [ ] 7.1 Run full build (`./gradlew build`) and fix any compilation errors or -Xlint warnings introduced by the new API
- [ ] 7.2 Update the example pipelines in `flowpipe-test` to demonstrate at least one use of `Step.builder()`, `Result.map()`, and `.when()` where they improve readability
- [ ] 7.3 Update README to document the new factory methods and fluent `Result` API with short code samples
