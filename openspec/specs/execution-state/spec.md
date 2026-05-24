# execution-state

## Purpose

Defines the three-tier data model surface — typed step input/output as the primary channel, `State` as a mutable execution-scoped store keyed by typed `StateKey<T>`, and `RequestContext` as an immutable per-execution read-only store keyed by typed `ContextKey<T>` — and the isolation guarantees between executions that make the library safe to embed in any runtime, including AWS Lambda.

## Requirements

### Requirement: State is execution-scoped and mutable

The library SHALL provide a `State` object, a fresh instance of which is constructed for every `Pipeline.execute(...)` invocation. `State` SHALL be readable and writable by any step within that execution and SHALL NOT be shared across executions. Holding a reference to a `State` after its `execute(...)` call returns MUST NOT affect any subsequent execution.

#### Scenario: Two executions of the same pipeline have independent state

- **WHEN** a single built `Pipeline` is executed twice in sequence and the first execution's first step writes a value to `state`
- **THEN** the second execution's first step MUST observe `state` as empty for that key

### Requirement: State uses typed keys

`State` reads and writes SHALL go through typed `StateKey<T>` instances. `state.get(StateKey<T>)` SHALL return a value of type `T` (or `null` if absent) with no cast required at the call site. `state.set(StateKey<T>, T)` SHALL accept only values assignable to `T`.

#### Scenario: Get returns the typed value

- **WHEN** a step writes `state.set(USER_KEY, user)` where `USER_KEY` is a `StateKey<User>`, and a later step calls `state.get(USER_KEY)`
- **THEN** the later step receives a value typed as `User` with no cast required

#### Scenario: Get on an absent key returns null

- **WHEN** a step calls `state.get(SOME_KEY)` for a key that no prior step has set
- **THEN** the call MUST return `null`

### Requirement: RequestContext is per-execution and immutable

The library SHALL provide a `RequestContext` object constructed once per `Pipeline.execute(...)` invocation from values supplied by the caller. `RequestContext` SHALL expose only read accessors and MUST NOT permit mutation by any step. Two executions of the same pipeline with different request contexts SHALL be fully isolated.

#### Scenario: RequestContext has no public mutator

- **WHEN** a step receives a `RequestContext` from its `StepContext`
- **THEN** the `RequestContext` type MUST NOT expose any public method that mutates its contents

#### Scenario: Two executions see distinct request contexts

- **WHEN** a pipeline is executed twice with two different `RequestContext` instances, each carrying a distinct trace id under the same `ContextKey<String> TRACE_ID`
- **THEN** each execution's steps observe their own trace id and neither execution sees the other's

### Requirement: RequestContext uses typed keys

`RequestContext` reads SHALL go through typed `ContextKey<T>` instances. `context.get(ContextKey<T>)` SHALL return a value of type `T` (or `null` if absent) with no cast required at the call site.

#### Scenario: Context get returns the typed value

- **WHEN** a caller builds a `RequestContext` with `ContextKey<String> TRACE_ID` mapped to `"abc-123"` and a step calls `context.get(TRACE_ID)`
- **THEN** the step receives the value `"abc-123"` typed as `String` with no cast required

### Requirement: No static, global, or thread-local state

The library MUST NOT use any static mutable fields, thread-local storage, or other process-global mechanisms to convey `State`, `RequestContext`, or step input/output between steps. All such data SHALL flow exclusively through the `StepContext` and step input/output parameters.

#### Scenario: Inspection finds no thread-local or static carriers

- **WHEN** the library source is inspected for static mutable fields and `ThreadLocal` usages in `flowpipe-core`
- **THEN** none MUST exist for the purposes of conveying execution state, request context, or step data
