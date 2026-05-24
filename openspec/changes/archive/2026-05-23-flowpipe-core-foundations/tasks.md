## 1. Project scaffold

- [x] 1.1 Create root `settings.gradle.kts` declaring the two subprojects (`flowpipe-core`, `flowpipe-test`)
- [x] 1.2 Create root `build.gradle.kts` with the Java 17 toolchain, common repositories, and shared `java-library` conventions
- [x] 1.3 Add the Gradle wrapper (current stable version) and commit `gradlew`, `gradlew.bat`, `gradle/wrapper/`
- [x] 1.4 Create `flowpipe-core/build.gradle.kts` declaring `slf4j-api` as the sole runtime dep and JUnit 5 + AssertJ as test deps
- [x] 1.5 Create `flowpipe-test/build.gradle.kts` depending on `flowpipe-core` (api scope) and re-exporting JUnit 5 + AssertJ
- [x] 1.6 Add a `.gitignore` covering `build/`, `.gradle/`, `*.iml`, `.idea/`, `.vscode/`
- [x] 1.7 Verify `./gradlew build` succeeds against the empty modules

## 2. Core type surface in `flowpipe-core`

- [x] 2.1 Create package `io.flowpipe.api` and `io.flowpipe.state`
- [x] 2.2 Implement `StateKey<T>` (final class, holds `String name` and `Class<T> type`, equality on name)
- [x] 2.3 Implement `ContextKey<T>` mirroring `StateKey<T>` semantics
- [x] 2.4 Implement `State` (mutable, backed by `Map<StateKey<?>, Object>`, typed `get`/`set` returning `T`, returns `null` for absent keys, not thread-safe by design)
- [x] 2.5 Implement `RequestContext` (immutable, backed by an unmodifiable map, typed `get` returning `T` or `null`, builder `RequestContext.builder().put(key, value).build()`, `RequestContext.empty()` constant)
- [x] 2.6 Implement `StepContext` interface exposing `State state()` and `RequestContext context()`; provide an internal default implementation in `io.flowpipe.engine`

## 3. Step abstraction

- [x] 3.1 Create package `io.flowpipe.validation`; implement `Validator<T>` interface (`void validate(T value) throws ValidationException`) and `ValidationException` (unchecked, carries a message)
- [x] 3.2 Implement `NoOpValidator` as a stateless singleton with a typed `instance()` accessor
- [x] 3.3 Implement `StepDescriptor<I, O>` as a record carrying `id`, `inputType`, `outputType`, `inputValidator`, `outputValidator`; provide a builder with `NoOpValidator` defaults
- [x] 3.4 Define `Step<I, O>` interface with `StepDescriptor<I, O> describe()` and `O execute(I, StepContext) throws Exception`
- [x] 3.5 Provide `Step.of(id, inputType, outputType, BiFunction<I, StepContext, O>)` convenience factory for ad-hoc steps

## 4. Result type

- [x] 4.1 Create `TraceEntry` record with `stepId`, `startedAtNanos`, `durationNanos`, `attempts`
- [x] 4.2 Create `ExecutionTrace` value type wrapping an immutable `List<TraceEntry>`; expose `entries()` and a package-private mutable builder used by the engine
- [x] 4.3 Define `Result<O>` sealed interface permitting `Success<O>` and `Failure<O>`
- [x] 4.4 Define `Success<O>` record with `value` and `trace`; `Failure<O>` record with `cause`, `failedStepId`, `trace`

## 5. Pipeline + builder + engine

- [x] 5.1 Create package `io.flowpipe.engine`; define `PipelineBuildException` (unchecked)
- [x] 5.2 Implement `PipelineBuilder<I, O>` with `.start(Class<I>)` static factory, `.then(Step<O, X>)` returning `PipelineBuilder<I, X>`, and a single-shot `build()` that throws `IllegalStateException` on reuse
- [x] 5.3 Implement `Pipeline<I, O>` as immutable, holding `inputType`, `outputType`, and the ordered step list; expose `inputType()`, `outputType()`, `execute(I input, RequestContext ctx)`, and an `execute(I)` convenience that supplies `RequestContext.empty()`
- [x] 5.4 Implement the synchronous execution engine: for each step in order — validate input, capture `startedAtNanos` (use `System.nanoTime()`), call `execute`, validate output, capture `durationNanos`, append a `TraceEntry`; on any thrown `Throwable` (including `ValidationException`) build and return a `Failure` with the originating step id and the partial trace
- [x] 5.5 Implement the build-time validation rules: reject empty pipelines (in `build()`), reject duplicate step ids (in `build()`), reject step-to-step `inputType`/cursor mismatches (in `.then()`)

## 6. `flowpipe-test` module

- [x] 6.1 Implement `StepHarness` with a fluent API for invoking a single `Step<I, O>` under a caller-supplied `RequestContext` and an optionally-prepopulated `State`, returning the step's raw output or wrapping a thrown exception
- [x] 6.2 Implement `Steps` test factories for building no-op, identity, and throwing steps used widely in tests
- [x] 6.3 Add a smoke test exercising `StepHarness` against an identity step

## 7. Spec-driven test coverage in `flowpipe-core`

- [x] 7.1 Tests for `pipeline-composition`: compatible-chain compile (positive), build rejects empty pipeline, build rejects duplicate ids, `.then()` rejects step-to-step type mismatch from raw-type escape, builder rejects reuse after `build()` and double `build()`
- [x] 7.2 Tests for `step-execution`: input validation failure prevents `execute` and yields `Failure` with correct `failedStepId`, output validation failure halts pipeline and skips subsequent steps, `NoOpValidator` defaults allow `null` and any value, `Step.execute` exceptions surface as `Failure` with cause and id, `StepContext.state()` and `context()` are non-null even with empty context
- [x] 7.3 Tests for `execution-state`: two executions of one pipeline have isolated `State`, typed `StateKey` get/set with no casts, `State.get` returns `null` for absent keys, `RequestContext` exposes no mutator (reflective check on its public API), two executions see distinct request contexts
- [x] 7.4 Tests for `pipeline-result`: pattern-matching `instanceof` over `Result` handles both arms without a cast, `Success` carries the final value and an ordered trace, `Failure` trace contains entries for every executed step up to and including the failing one and no later steps, every `TraceEntry.attempts()` equals `1`
- [x] 7.5 Add an architectural test that fails the build if any class in `io.flowpipe.*` declares a static mutable field or a `ThreadLocal` (uses a simple reflection scan over compiled classes, no extra dep)

## 8. Documentation and CI hygiene

- [x] 8.1 Update `CLAUDE.md`: replace the "Planned structure" section with the actually-shipped module + package layout, and add the build commands (`./gradlew build`, `./gradlew :flowpipe-core:test --tests <fqn>`)
- [x] 8.2 Add a top-level `README.md` containing a single end-to-end example (define two steps, chain them, execute, switch on the `Result`) — kept under one page
- [x] 8.3 Verify the full build is green: `./gradlew clean build` from a fresh checkout
