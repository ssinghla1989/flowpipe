## Why

Defining steps and handling results in FlowPipe requires more ceremony than the abstractions warrant — checked exceptions must be wrapped unnecessarily, `Result` values require instanceof pattern-matching boilerplate at every call site, and there is no fluent way to define a step with inline policies or apply a conditional step without building a full sub-pipeline. These friction points accumulate and become the first things a new developer notices.

## What Changes

- **`Step.ofChecked()`** — a new factory method on `Step` accepting a body that declares `throws Exception`, eliminating the need to wrap checked exceptions in `RuntimeException` inside `Step.of()` lambdas.
- **`Result` fluent API** — new methods on `Result<O>`: `.map()`, `.flatMap()`, `.fold()`, `.getOrElse()`, `.getOrThrow()` — covering the most common call-site patterns without requiring pattern matching.
- **`Step.builder()` DSL** — a fluent builder that unifies step definition and inline policy declaration, so a step with retry, timeout, and circuit-breaker policies can be constructed in one expression.
- **`.when(predicate, step)` shorthand** — a `PipelineBuilder` method that wraps a single step in a pass-through conditional, removing the need to construct a full `Pipeline<O,O>` arm just to express "apply this step only if…".
- **`Pipeline.builder()` and `Pipeline.of()` static aliases** — discoverable entry points on `Pipeline` itself, alongside `PipelineBuilder.start()`.

## Capabilities

### New Capabilities

- `step-factory-checked`: `Step.ofChecked()` factory and supporting functional interface for checked-exception bodies.
- `result-fluent-api`: Fluent transformation and extraction methods on `Result<O>`.
- `step-builder-dsl`: `Step.builder()` fluent DSL for defining steps with inline policies.
- `pipeline-builder-when`: `.when(predicate, step)` shorthand on `PipelineBuilder`.
- `pipeline-entry-points`: `Pipeline.builder()` / `Pipeline.of()` discoverable static aliases.

### Modified Capabilities

## Impact

- `io.flowpipe.api.Step` — new `ofChecked()` static method; new `builder()` static method.
- `io.flowpipe.api.Result` — new default/static methods (`.map()`, `.flatMap()`, `.fold()`, `.getOrElse()`, `.getOrThrow()`); **additive only**, no breaking changes to existing sealed record types.
- `io.flowpipe.engine.PipelineBuilder` — new `.when()` method and static `builder()` alias on `Pipeline`.
- New `io.flowpipe.api.CheckedBiFunction` functional interface (or internal equivalent) to support `ofChecked()`.
- All changes are **additive** — no existing API is modified or removed.
- `flowpipe-test` — `Steps.noop()` updated to use the `ofChecked` pattern or a type that avoids the null-return bug.
