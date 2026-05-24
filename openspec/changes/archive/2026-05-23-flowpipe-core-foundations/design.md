## Context

FlowPipe will be a Java library for orchestrating synchronous API pipelines, deliberately patterned on Mastra's workflow engine (with the divergences documented in `CLAUDE.md`). This change establishes the foundation: project scaffolding, the core type surface, and the build-time validation seam (`Pipeline.build()`) that everything downstream relies on.

The hard constraints from `CLAUDE.md` are load-bearing here:
- No Spring dependency in core.
- Library must run unchanged on AWS Lambda and EC2 — no background threads outliving a request, no daemon processes.
- Step authors write zero logging code and zero metrics code; both are framework concerns wrapped around `execute`.
- Input validated before each step, output after.
- Build-time guarantees beat runtime checks.

This is the first source code in the repository. Every decision here sets a precedent.

## Goals / Non-Goals

**Goals:**
- A working multi-module Gradle build with `flowpipe-core` and `flowpipe-test`.
- A typed, generics-driven `Step` + `Pipeline` + `PipelineBuilder` API that makes mis-wired sequential chains a compile error and other wiring mistakes a `build()`-time error.
- Three-tier data model surface (`I/O`, `State`, `RequestContext`) with typed keys for the two stores.
- Sealed `Result<O>` so callers exhaustively switch on success/failure.
- Validator SPI so consumers plug in their own validation library (or none).
- Zero hidden runtime cost: no reflection in the hot path, no thread pools created during `build()`.

**Non-Goals:**
- Parallel composition, branching, loops, `map()` — all deferred to later phases.
- Retry, backoff, lifecycle hooks (`onError`/`onFinish`) — Phase 4.
- Metrics emission and structured-log emission — Phase 2. (This change installs the seams: the engine invokes per-step lifecycle around each `execute` so Phase 2 only adds emitters.)
- A Micrometer, Jackson, or Spring adapter module.
- Streaming, suspend/resume, durable execution — explicit non-goals for the whole project.
- Performance tuning or benchmarking. Correctness first; benchmarks come once we have something to compare.

## Decisions

### D1: Java 17, Gradle Kotlin DSL

Java 17 is the LTS available on every current Lambda runtime (`java17`, `java21`) and on every supported EC2 AMI. It gives us sealed interfaces and records, both of which we use directly (`Result`, `ExecutionTrace` entries).

Gradle Kotlin DSL over Maven: type-safe build scripts, better multi-module ergonomics, easier to extend later with the planned `flowpipe-micrometer`/`flowpipe-spring` modules without retrofitting profiles.

**Alternatives considered:** Java 21 baseline (rejected — narrower deployment compatibility for users; we can raise later as a minor-version bump). Maven (rejected — clunkier multi-module wiring, no compile-time build-script feedback).

### D2: One mandatory runtime dependency in core: `slf4j-api`

SLF4J is the de facto Java logging API. Depending on the API (not a backend) keeps consumers free to bring Logback, Log4j2, or anything else. This is the only dep we add to core in this change; metrics stay behind an in-house SPI to avoid forcing Micrometer.

**Alternatives considered:** JUL (rejected — universally avoided in practice). System.Logger from JDK 9+ (rejected — adoption is poor; downstream tooling still expects SLF4J).

### D3: `Step<I, O>` as a single-method interface, not an abstract class

```java
public interface Step<I, O> {
    StepDescriptor<I, O> describe();
    O execute(I input, StepContext ctx) throws Exception;
}
```

A functional-interface-shaped contract keeps simple steps lambda-ish and complex steps class-ish. The `describe()` method returns an immutable `StepDescriptor<I, O>` with `id`, `Class<I> inputType`, `Class<O> outputType`, `Validator<I> inputValidator`, `Validator<O> outputValidator`. We carry the `Class<I>`/`Class<O>` because Java erases generics — without explicit class tokens, the engine can't perform output-type validation at `build()` time or wire to a validator.

Throwing `Exception` is deliberate. Step authors should not be forced to wrap checked exceptions; the engine catches anything thrown and produces a `Failure`.

**Alternatives considered:** Abstract class with template-method (rejected — couples the user's class hierarchy, harder to test). Reflective TypeToken capture à la Guice (rejected — fragile and brittle on Lambda's class-loading quirks; explicit class tokens are simple and unambiguous).

### D4: Typed keys for `State` and `RequestContext`

```java
public final class StateKey<T> { String name; Class<T> type; }
public final class ContextKey<T> { String name; Class<T> type; }
```

`state.get(USER_KEY)` returns `User` without a cast. The alternative — `Map<String, Object>` — pushes casts and class-cast bugs onto step authors. Typed keys cost a few lines and pay back at every call site.

`RequestContext` is built once per `pipeline.execute(...)` call and is **immutable** (backed by an unmodifiable map). `State` is mutable but execution-scoped — a fresh instance per `execute` call. There are no static, shared, or long-lived holders, which preserves the Lambda contract.

**Alternatives considered:** ThreadLocal-backed context (rejected — incompatible with the future where step internals might dispatch async work, and a footgun in any pooled-thread environment). String-keyed Map (rejected — loses type safety, the whole point of the library).

### D5: `PipelineBuilder<I, O>` tracks the current output type in its generic parameter

```java
public final class PipelineBuilder<I, O> {
    public <X> PipelineBuilder<I, X> then(Step<O, X> next) { ... }
    public Pipeline<I, O> build() { ... }
}
```

`.then()` returns a new builder with the input type fixed and the output type advanced. Wiring a `Step<String, Integer>` after a step that produced `User` is a compile error. The builder is not reusable — `build()` consumes it and any further calls throw.

`PipelineBuilder.start(Class<I> inputType)` is the entry point; it primes the type chain with the input type. The terminal `Step<O, O>` whose output matches the desired pipeline output is the last `.then()`.

**Alternatives considered:** Single mutable `PipelineBuilder` with `Object`-typed internals plus runtime checking only (rejected — gives up the headline guarantee). A separate DSL class per arity (rejected — buys nothing over generic chaining and obscures the surface).

### D6: Build-time validation in `build()`

`build()` walks the recorded steps and rejects:
1. Empty pipeline (no `.then()` calls).
2. Duplicate step ids across the pipeline.

`.then()` itself additionally rejects:
3. A step whose declared `inputType` does not equal the builder's current output type. (Most cases caught by generics — this catches raw-type and reflection escapes. A single-step lie about output type is undetectable without an explicit caller-supplied expectation; the chain-consistency check catches every multi-step variant.)

Each failure throws `PipelineBuildException` with a message that names the offending step id(s) and the conflicting type names. The exception type is distinct from runtime failures so test suites can assert against build-time vs. run-time errors precisely.

This list is intentionally short for this slice; parallel-arity, branch-default-required, and state-key-conflict checks land alongside parallel/branch in later changes.

### D7: Sealed `Result<O>` with records

```java
public sealed interface Result<O> permits Success, Failure {}
public record Success<O>(O value, ExecutionTrace trace) implements Result<O> {}
public record Failure<O>(Throwable cause, String failedStepId, ExecutionTrace trace) implements Result<O> {}
```

Callers on Java 17 use pattern-matching `instanceof` to discriminate variants without casts: `if (result instanceof Success<O> s) { ... } else if (result instanceof Failure<O> f) { ... }`. Switch-expression exhaustiveness over sealed types stabilised in Java 21; consumers on 21+ get that for free as a consequence of the sealing. Either way: no `result.isSuccess()` boolean to forget to check; no `null` on failure paths.

`ExecutionTrace` is `List<TraceEntry>` where `TraceEntry` records `stepId`, `startedAtNanos`, `durationNanos`, `attempts`. `attempts` is always `1` in this slice — Phase 4 will populate it for real.

**Alternatives considered:** Throwing exceptions out of `Pipeline.execute` (rejected — failures are expected, not exceptional, and forcing try/catch at every call site is the boilerplate FlowPipe is meant to eliminate). `Optional<O>` + `Throwable` pair (rejected — three-state result is awkward without sealing).

### D8: Validation runs inside the engine, around each `execute`

The engine, not the step, calls `descriptor.inputValidator().validate(input)` before `step.execute(...)` and `descriptor.outputValidator().validate(output)` after. A validation failure is caught and reported as a `Failure` with the originating step's id. This keeps `Step.execute` implementations one method long and prevents "trust the previous step" shortcuts.

`Validator<T>` is a single-method interface `void validate(T value) throws ValidationException`. `NoOpValidator.instance()` is the default supplied when a step omits validators.

**Alternatives considered:** Jakarta Bean Validation as the default (rejected — drags in Hibernate Validator transitively, which is a heavy dep to force). Returning a Validation result object (rejected — exception flow integrates cleanly with the engine's existing failure path).

### D9: Engine is synchronous and single-threaded in this slice

There is one execution thread per `Pipeline.execute(...)` call — the caller's thread. No `ExecutorService` is created or referenced. This is the cleanest fit for Lambda (one request per cold container instance, never share state across invocations) and avoids any background-thread lifecycle concern. Parallel composition will introduce an executor — but it will be injected at `build()` time and its lifecycle is the caller's, not FlowPipe's.

## Risks / Trade-offs

- **Class tokens (`Class<I>`, `Class<O>`) don't capture parameterized types** (e.g., `List<User>` erases to `List`). → Mitigation: documented as a known limitation; users wanting parameterized I/O must wrap in a named record type. We will revisit (likely a `TypeRef<T>` super-type-token) when a real use case demands it; not in this slice.
- **`build()`-time checks are limited in this slice** — most are caught by generics, only the runtime escapes are caught. → Mitigation: each subsequent change explicitly extends the `build()` rule set; the test suite enforces the rules added each slice.
- **Sealed interfaces require Java 17 in consumer projects too**, since `permits` is part of the public ABI. → Mitigation: explicitly documented as the floor; Java 17 is already the LTS for every realistic deployment target.
- **Synchronous engine has no concurrency safety net** — a step that spawns its own threads can violate the Lambda contract. → Mitigation: documented in `CLAUDE.md` hard constraints; no API affordance encourages it. We can add a runtime check (e.g., reject `Thread.currentThread().isDaemon()` mid-execute) later if it becomes a real problem.
- **`StepDescriptor` is duplicative for trivial steps** (you state input/output types twice — in the generic parameters and in the descriptor). → Mitigation: provide a `Step.of(id, Class<I>, Class<O>, BiFunction<I, StepContext, O>)` convenience for ad-hoc cases; full-class steps just accept the modest verbosity.

## Migration Plan

Not applicable — first source code in the repository, no prior API, no consumers, nothing to migrate.

## Open Questions

None blocking. Two to revisit in a later change:
- Whether to add a `TypeRef<T>` super-type-token for parameterized I/O (waits for a real use case).
- Whether `RequestContext` should be required (currently `Pipeline.execute(input)` overload uses an empty context). Leaning toward "always required, even if empty" for explicitness — flag for Phase 2.
