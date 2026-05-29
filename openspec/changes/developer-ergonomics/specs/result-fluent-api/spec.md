## ADDED Requirements

### Requirement: Result.map transforms Success value and passes through Failure
`Result<O>` SHALL provide a default method `map(Function<O, X> fn)` returning `Result<X>`. When called on a `Success<O>`, it SHALL apply `fn` to the value and return a new `Success<X>`. When called on a `Failure<O>`, it SHALL return the same `Failure` re-typed to `Failure<X>` without invoking `fn`. If `fn` throws an unchecked exception, that exception propagates to the caller; `map` does not catch it.

#### Scenario: Success value is transformed
- **WHEN** `result.map(v -> v.toUpperCase())` is called on `Success("hello")`
- **THEN** the result is `Success("HELLO")`

#### Scenario: Failure passes through untouched
- **WHEN** `result.map(v -> v.toUpperCase())` is called on a `Failure` with cause `RuntimeException("bad")`
- **THEN** the result is the same `Failure` — cause and failedStepId unchanged, `fn` not invoked

### Requirement: Result.flatMap transforms Success to another Result
`Result<O>` SHALL provide a default method `flatMap(Function<O, Result<X>> fn)` returning `Result<X>`. When called on a `Success<O>`, it SHALL apply `fn` to the value and return the `Result<X>` that `fn` returns. When called on a `Failure<O>`, it SHALL return the same `Failure` re-typed without invoking `fn`.

#### Scenario: Success value is flatMapped to another Success
- **WHEN** `result.flatMap(v -> Result.success(v.length()))` is called on `Success("hello")`
- **THEN** the result is `Success(5)`

#### Scenario: Success value flatMapped to Failure propagates the Failure
- **WHEN** `result.flatMap(v -> Failure("parse", new RuntimeException("bad"), trace))` is called on a `Success`
- **THEN** the result is the `Failure` returned by `fn`

#### Scenario: Failure passes through without invoking fn
- **WHEN** `flatMap` is called on a `Failure`
- **THEN** the original `Failure` is returned and `fn` is not called

### Requirement: Result.fold collapses to a value via two functions
`Result<O>` SHALL provide a default method `fold(Function<O, X> onSuccess, Function<Failure<O>, X> onFailure)` returning `X`. When called on `Success`, it SHALL invoke `onSuccess` with the value. When called on `Failure`, it SHALL invoke `onFailure` with the `Failure` instance. Exactly one function is invoked per call.

#### Scenario: Success branch is taken on Success
- **WHEN** `result.fold(v -> "ok:" + v, f -> "err:" + f.failedStepId())` is called on `Success("x")`
- **THEN** the return value is `"ok:x"` and `onFailure` is never called

#### Scenario: Failure branch is taken on Failure
- **WHEN** `result.fold(v -> "ok:" + v, f -> "err:" + f.failedStepId())` is called on a `Failure` with `failedStepId="step-1"`
- **THEN** the return value is `"err:step-1"` and `onSuccess` is never called

### Requirement: Result.getOrElse returns value or fallback
`Result<O>` SHALL provide a default method `getOrElse(O defaultValue)` that returns the `Success` value when successful, or `defaultValue` when failed. A second overload `getOrElse(Supplier<O> supplier)` SHALL also be provided, evaluating `supplier` lazily only on `Failure`.

#### Scenario: Returns value on Success
- **WHEN** `Success("hello").getOrElse("default")` is called
- **THEN** the result is `"hello"`

#### Scenario: Returns fallback on Failure
- **WHEN** `failure.getOrElse("default")` is called on a `Failure`
- **THEN** the result is `"default"`, regardless of failure cause

#### Scenario: Supplier is not evaluated on Success
- **WHEN** `Success("hello").getOrElse(() -> { throw new RuntimeException(); })` is called
- **THEN** no exception is thrown and the result is `"hello"`

### Requirement: Result.getOrThrow extracts value or throws
`Result<O>` SHALL provide a default method `getOrThrow()` that returns the `Success` value when successful, or throws a `RuntimeException` wrapping `Failure.cause()` when failed. A second overload `getOrThrow(Function<Failure<O>, E> exMapper) throws E` SHALL also be provided, allowing callers to map the failure to a typed exception.

#### Scenario: Returns value on Success
- **WHEN** `Success("hello").getOrThrow()` is called
- **THEN** the return value is `"hello"` and no exception is thrown

#### Scenario: Throws RuntimeException on Failure with no mapper
- **WHEN** `failure.getOrThrow()` is called on a `Failure` whose cause is `IOException`
- **THEN** a `RuntimeException` is thrown whose cause is the original `IOException`

#### Scenario: Throws mapped exception with custom mapper
- **WHEN** `failure.getOrThrow(f -> new MyApiException(f.failedStepId()))` is called on a `Failure`
- **THEN** a `MyApiException` is thrown with the failed step id, not wrapped in `RuntimeException`
