## Why

FlowPipe has no implementation yet — only intent documented in `CLAUDE.md`. Before the library can deliver its value proposition (sequential, parallel, and conditional pipeline composition with automatic validation, retry, and observability), the foundational module structure, core type abstractions, and the build-time wiring-validation seam must exist. Without these, every later feature has nowhere to land.

## What Changes

- Scaffold a Gradle (Kotlin DSL) multi-module project rooted at the repository: `flowpipe-core` and `flowpipe-test`.
- Pin Java 17 as the toolchain baseline.
- Introduce the public `Step<I, O>` abstraction with a single `execute(I, StepContext)` method and a `StepDescriptor` carrying id, input class, output class.
- Introduce the three-tier data model surface: typed step input/output, a mutable execution-scoped `State` keyed by typed `StateKey<T>`, and an immutable per-execution `RequestContext` keyed by typed `ContextKey<T>`.
- Introduce a sealed `Result<O>` type with `Success<O>` and `Failure<O>` variants, each carrying an `ExecutionTrace`.
- Introduce a fluent `PipelineBuilder<I, O>` exposing only `.then(Step<O, X>)` and `.build()` in this slice. Generic state-tracking ensures type-incompatible chains fail to compile.
- Introduce a built, immutable `Pipeline<I, O>` whose `execute(I, RequestContext)` returns a `Result<O>`.
- Introduce a `Validator<T>` SPI with a `NoOpValidator` default. Inputs are validated before each step's `execute`; outputs validated after. Validation failures produce a `Failure` result.
- Implement build-time validation in `Pipeline.build()` for the rules expressible at this slice: empty pipeline, declared-vs-actual output type mismatch, duplicate step ids.
- Establish the test module `flowpipe-test` with a `StepHarness` for running individual steps under a fake `StepContext`, and the JUnit 5 + AssertJ test stack.
- Add a working build: `./gradlew build` compiles and runs tests; CLAUDE.md updated with the real commands and shipped package layout.

No Spring, no Micrometer, no Jackson — `flowpipe-core` ships with one runtime dependency: `slf4j-api` (used only as a compile-time API surface in this slice; emission lands in a later change).

## Capabilities

### New Capabilities
- `pipeline-composition`: Defines how a developer composes typed `Step`s into an immutable, executable `Pipeline` via a fluent `PipelineBuilder`, and the build-time guarantees the builder makes (type chain integrity, no empty pipelines, unique step ids).
- `step-execution`: Defines the `Step<I, O>` contract — what its single `execute` method receives (typed input, `StepContext` exposing `State` and `RequestContext`), what it must return, and how input/output validation wraps every invocation.
- `execution-state`: Defines the three-tier data model surface — typed step I/O as the primary channel, `State` as mutable execution-scoped store keyed by typed `StateKey<T>`, and `RequestContext` as immutable per-execution read-only store keyed by typed `ContextKey<T>`.
- `pipeline-result`: Defines the sealed `Result<O>` returned from `Pipeline.execute`, its `Success`/`Failure` variants, the structure of `ExecutionTrace`, and the rule that a step throwing terminates the pipeline as a `Failure` carrying the failing step id.

### Modified Capabilities
<!-- None — no prior specs exist in this repository. -->

## Impact

- **New code**: First source code in the repository. Establishes the package root (`io.flowpipe.api`, `io.flowpipe.engine`, `io.flowpipe.validation`, `io.flowpipe.state`) and the test module.
- **Build system**: Introduces Gradle Kotlin DSL build scripts, a `gradle/wrapper`, and a root `settings.gradle.kts`. No prior build system to migrate from.
- **Dependencies**: One runtime dep added — `slf4j-api`. Test deps: JUnit 5, AssertJ. No transitive surprises.
- **Documentation**: `CLAUDE.md` updated to replace the "planned structure" section with the actually-shipped layout and to add real build/test commands.
- **Downstream changes unblocked**: Phase 2 (observability emission), Phase 3 (parallel/branch), Phase 4 (retry/hooks) all depend on the abstractions introduced here. Each will be proposed as its own change.
- **No breaking changes** — there is no prior API to break.
