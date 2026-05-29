## Context

FlowPipe's public API is clean and principled, but five friction points appear in every non-trivial pipeline:

1. `Step.of()` uses `BiFunction<I, StepContext, O>` — no `throws` clause — so any step body that calls a checked-exception API must swallow it in `RuntimeException`. The full `Step` interface allows `execute() throws Exception`, so this is a gap only in the factory.
2. `Result<O>` is a sealed interface with two records (`Success`, `Failure`). Every call site that wants to extract a value or handle an error does `instanceof Success<O> s` / `instanceof Failure<O> f` pattern matching — three to six lines of boilerplate that convey no intent.
3. Steps with resilience policies are declared by chaining `Step.of().withRetry().withTimeout()`. Each call creates a new anonymous wrapper object. There is no single coherent expression for "a step with an id, a body, and these policies".
4. Single-armed conditional execution requires `PipelineBuilder.branch(id, predicate, Pipeline<O,O>)`. The arm must be a full `Pipeline<O,O>` — a separate `PipelineBuilder.start().then(step).build()` expression — just to say "apply this one step conditionally".
5. The entry point `PipelineBuilder.start(Class<I>)` is not discoverable from the `Pipeline` class, which is where most users start reading Javadoc.

All five are purely additive changes — no existing type is modified in a breaking way.

## Goals / Non-Goals

**Goals:**

- `Step.ofChecked()` factory accepts a body that may throw checked exceptions.
- `Result<O>` gains `.map()`, `.flatMap()`, `.fold()`, `.getOrElse()`, `.getOrThrow()` — fluent methods that eliminate instanceof boilerplate at call sites.
- `Step.builder()` DSL produces a step with all policies declared in one expression.
- `.when(predicate, step)` on `PipelineBuilder` wraps a single step in a pass-through conditional without requiring a sub-pipeline.
- `Pipeline.builder(Class<I>)` and `Pipeline.of(...)` as static aliases on `Pipeline`.
- All new API lives in `io.flowpipe.api` (or `io.flowpipe.engine` for builder aliases). No new modules. No new runtime dependencies.

**Non-Goals:**

- Monad laws / functor laws compliance for `Result` — this is a convenience layer, not a functional programming library.
- `Result.flatMap()` chaining pipeline execution — `flatMap` maps `Success<O>` to another `Result<X>` (e.g., parse/validate the value), not to another pipeline execution.
- Any modification to the execution engine (`Pipeline`, `PipelineBuilder` internals) beyond the `.when()` shorthand.
- Changing existing `Step.of()`, `withRetry()`, `withTimeout()`, or `withCircuitBreaker()` defaults — they remain as-is.

## Decisions

### D1: `Step.ofChecked()` — separate method vs. overload

**Decision:** Add `Step.ofChecked(String, Class<I>, Class<O>, CheckedBiFunction<I, StepContext, O>)` as a new static method. Keep `Step.of()` unchanged.

**Alternatives considered:**
- Overload `Step.of()` with a checked functional interface — rejected because `BiFunction` and `CheckedBiFunction` would create an overload ambiguity for lambda bodies, requiring casts at call sites.
- Replace `Step.of()` with the checked variant — rejected; would be a breaking change and the unchecked variant is fine for pure/in-memory step bodies.

**`CheckedBiFunction<I, C, O>`:** A package-private or public `@FunctionalInterface` in `io.flowpipe.api` declaring `O apply(I input, C ctx) throws Exception`. Named `CheckedBiFunction` to signal it's a checked analog of `BiFunction`.

---

### D2: `Result` fluent API — methods on the interface vs. utility class

**Decision:** Add default methods directly to the `Result<O>` sealed interface.

**Alternatives considered:**
- Static utility class `Results` — rejected; it splits the API and makes discovery harder.
- Separate `ResultOps` wrapper — rejected; unnecessary indirection.

**Method signatures:**
```java
// Transform Success value; Failure passes through unchanged
<X> Result<X> map(Function<O, X> fn);

// Transform Success to another Result (e.g., parse/validate)
<X> Result<X> flatMap(Function<O, Result<X>> fn);

// Collapse to a value — one branch for success, one for failure
<X> X fold(Function<O, X> onSuccess, Function<Failure<O>, X> onFailure);

// Extract value or substitute
O getOrElse(O fallback);
O getOrElse(Supplier<O> fallbackSupplier);

// Extract value or throw
O getOrThrow();                          // throws RuntimeException wrapping cause
<E extends Throwable> O getOrThrow(Function<Failure<O>, E> exMapper) throws E;
```

`map()` and `flatMap()` implementations on `Success` delegate to `fn`; on `Failure` they return `this` (or a re-typed `Failure`). Neither catches exceptions from `fn` — if `fn` throws, the exception propagates. This is intentional: the fluent methods are for data transformation, not pipeline execution.

---

### D3: `Step.builder()` DSL structure

**Decision:** `Step.builder(String id, Class<I> inputType, Class<O> outputType)` returns a `StepBuilder<I, O>` with `.execute(body)` (required), `.withRetry()`, `.withTimeout()`, `.withCircuitBreaker()`, `.withOutputKey()`, `.withInputValidator()`, `.withOutputValidator()`, and `.build()`.

The `StepBuilder` holds a `StepDescriptor` under construction and a separately-captured body function. `.build()` returns a `Step<I, O>` backed by both.

**Checked exception support:** The `.execute()` method on `StepBuilder` accepts a `CheckedBiFunction<I, StepContext, O>` — so `Step.builder()` subsumes the `Step.ofChecked()` use case cleanly. `Step.ofChecked()` is still added as a convenience factory for the minimal case.

**Alternatives considered:**
- `Step.of()` returning a builder — rejected; changes the meaning of a method that currently returns a `Step`, creating confusion.
- Adding policy methods directly to the `Step.of()` return value — these already exist as `withRetry()` etc., but they don't unify with step construction.

---

### D4: `.when(predicate, step)` — builder-level vs. step wrapper

**Decision:** Add `.when(String id, BiPredicate<O, StepContext> predicate, Step<O, O> step)` directly on `PipelineBuilder`. Internally this builds the pass-through pipeline and delegates to the existing single-armed `branch(id, predicate, Pipeline<O,O>)`.

**Why `String id`:** Branch nodes need an ID for trace/metrics/observability. Making it explicit keeps the same invariant as `.branch()`. Omitting it would require auto-generating IDs, which harms debuggability.

**Alternatives considered:**
- `.when()` as a method on `Step` returning a `Step<O, O>` wrapper — rejected because this bypasses the PipelineBuilder type system and cannot emit proper branch trace entries or observability.
- `.when()` without an id, using the step's id as the branch id — rejected; step id and branch id are different concepts, conflating them would cause duplicate-ID violations if the step appears elsewhere.

---

### D5: `Pipeline.builder()` alias placement

**Decision:** Add `Pipeline.builder(Class<I> inputType)` as a static method on `Pipeline` that delegates to `PipelineBuilder.start(inputType)`. Return type is `PipelineBuilder<I, I>`.

Also add `Pipeline.of(Class<I>, Step<I,O>, ...)` convenience factories for the common 1-step and 2-step cases, if the added surface area is judged worthwhile. (Keep this optional; the main value is `Pipeline.builder()`.)

## Risks / Trade-offs

**`Result.map()` exceptions propagate unchecked** → Mitigation: document clearly that `map()` / `flatMap()` do not catch exceptions from the supplied function. Users who want error-safe transformation should use `.fold()` or handle errors explicitly before calling `map()`.

**`Step.builder()` API surface expansion** → New `StepBuilder` class in the public API. Mitigation: mark it `@SuppressWarnings("unused")` friendly; it's additive. If the design is wrong in v1, `Step.builder()` can evolve independently of the `Step` interface.

**`.when()` convenience could mask intent** → `.when(id, pred, step)` is syntactic sugar for a single-step branch. Developers may try to pass a multi-step step (via `Pipeline.asStep()`) — this works correctly, but complex control flow should still use `.branch()` for clarity. Mitigation: Javadoc guidance.

**`Pipeline.builder()` discoverability vs. `PipelineBuilder.start()` coexistence** → Two entry points mean two things to find in docs. Mitigation: `PipelineBuilder.start()` Javadoc points to `Pipeline.builder()`; both are equally supported.

## Open Questions

- **`Pipeline.of()` convenience factories**: Are 1-step / 2-step `Pipeline.of(Class<I>, Step...)` factories worth the API surface? Leaning no for now — `Pipeline.builder().then(step).build()` is readable.
- **`Result.map()` and `null` from `fn`**: Should `map(fn)` check for null from `fn` and convert to `Failure`? Leaning no — stay consistent with Java's own stream/optional behavior; document the null contract.
