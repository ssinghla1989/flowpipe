# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

All planned features have landed. The current feature set:
- Gradle scaffold, `flowpipe-core`, `flowpipe-test`
- Core type surface: `Step`, `Pipeline`, `Result`, `State`, `RequestContext`
- Sequential composition with build-time wiring validation
- Observability emission: SLF4J structured logs (`step.start` / `step.finish` / `step.error`) and `MetricsRecorder` SPI around every step; `SpanRecorder` SPI for automatic distributed tracing around every step — register via `.withTracing(SpanRecorder)`, no lifecycle code needed
- Parallel composition: two overload families on `PipelineBuilder`:
  - **Combiner variants** `parallel2(Class<R>, combiner, stepA, stepB)` / `parallel3` / `parallel4` (typed combiners — `Class<R>` is the first argument and enables downstream type-compatibility checking at `build()` time), `parallelN(Class<R>, Map<String, Step<O,?>>, combiner)` (variadic with named branches); retry, timeout, and circuit-breaker policies declared on a parallel branch step are fully honored — the branch runs through the same resilience machinery as sequential steps
  - **Combiner-free variants** `parallel2(stepA, stepB)` / `parallel3` / `parallel4` / `parallelN(List<Step<O,?>>)` — each branch must declare `StepDescriptor.withOutputKey(StateKey<O>)` (enforced at `build()` time); branch outputs are auto-written to state after execution; the current pipeline value passes through unchanged to the next step; no holder type or combiner function needed
- Conditional branching: two overloads on `PipelineBuilder`:
  - **Two-armed** `branch(branchId, predicate, ifTrue, ifFalse)` — both arms must accept the current output type and produce the same output type; build-time type checking enforced
  - **Single-armed** `branch(branchId, predicate, ifTrue)` — false arm is a transparent pass-through; `ifTrue` must be `Pipeline<O, O>`; output type of the builder stays `O`; the pass-through step is named `branchId + ".pass-through"` and appears as a skipped trace entry when the true arm is taken
- Retry with backoff: `RetryPolicy` on `StepDescriptor`, configurable max attempts, initial delay, multiplier, and jitter; `.retryOn(Predicate<Throwable>)` restricts retries to exceptions matching the predicate — non-matching exceptions propagate immediately without consuming remaining attempts; returns a new immutable `RetryPolicy` instance
- Lifecycle hooks: `PipelineLifecycle<I, O>` SPI registered via `.withLifecycle(...)` on `PipelineBuilder`; `onStart` / `onFinish` / `onError` callbacks fire at the top-level pipeline boundary only
- Foreach fan-out: `.foreach(step)` / `.foreach(step, concurrency)` on `PipelineBuilder`; accepts a `List<E>` input and applies the step to each element, collecting `List<R>` output; concurrency > 1 runs elements in windowed parallel batches using the pipeline's executor
- Per-step execution timeout: `TimeoutPolicy` on `StepDescriptor` via `.withTimeout(...)`; `TimeoutPolicy.ofMillis(ms)` / `TimeoutPolicy.of(duration, unit)`; exceeded deadline surfaces as `Failure` with `StepTimeoutException` as the cause; each retry attempt receives its own independent deadline
- Circuit breaker policy: `CircuitBreakerPolicy` on `StepDescriptor` via `.withCircuitBreaker(...)`; CLOSED/OPEN/HALF-OPEN state machine with configurable failure-rate threshold, sliding window, open window, and half-open probe count; OPEN circuit fast-fails with `CircuitBreakerOpenException` before retry or timeout logic runs; state is per-`Pipeline`-instance, keyed by step id, persists across executions; circuit records the final retry-loop outcome (not individual attempt outcomes); applies to sequential steps, foreach steps, and parallel branch steps; `minimumCalls` is enforced as a floor on the failure count threshold — the circuit cannot open until at least `minimumCalls` failures have been observed regardless of the failure-rate threshold
- Pipeline introspection: `Pipeline.describe()` returns an immutable `PipelineDescriptor` (input/output types and an ordered list of `NodeDescriptor`s — `Step`, `Parallel`, `Branch`, `Foreach`). Branch arms recurse as nested `PipelineDescriptor`s; each step is exposed as its `StepDescriptor` so policies and validators are visible. Intended for diagnostics, audit logging, and tooling — never read during execution.
- Pipeline-level deadline: `.withDeadline(long ms)` / `.withDeadline(long duration, TimeUnit unit)` on `PipelineBuilder`; enforces a wall-clock total budget across the entire execution; checked before every node (sequential steps, parallel blocks, branches, foreach windows and items) and enforced while waiting for parallel branch futures and concurrent foreach item futures — slow branches or concurrent foreach windows cannot block past the deadline; surfaces as `Failure` with `PipelineDeadlineExceededException` (carrying `deadlineMs()`) and `failedStepId="pipeline.deadline"`; propagates into branch arms automatically; per-step `TimeoutPolicy` and the pipeline deadline are orthogonal.
- Null pipeline input rejection: `pipeline.execute(null, ...)` throws `NullPointerException` immediately, before lifecycle hooks fire or any step runs.
- Null step output detection: a step returning `null` from `execute()` surfaces immediately as `Failure` with a `NullPointerException` naming the offending step, before any output validation runs.
- Parallel combiner null-output detection: a user-supplied combiner returning `null` surfaces as `Failure` with a `NullPointerException` and `failedStepId="parallel.combiner"`, consistent with the step-output null contract.
- Skipped nested branch observability: when a `branch()` node is inside an arm that is skipped, `MetricsRecorder` and `SpanRecorder` receive the same `SKIPPED` callbacks for the branch node itself as for skipped step nodes, ensuring complete observability for deeply nested branch structures.
- Pipeline composition: `Pipeline.asStep(String id)` wraps any `Pipeline<I,O>` as a `Step<I,O>` so it can be used inside `.then()`, `.foreach()`, `.parallel2/3/4/N()`, or `.branch()` of another pipeline. The outer `RequestContext` is forwarded; inner `State` is isolated. When the inner pipeline fails, `Failure.cause()` is the original exception and `failedStepId()` is the adapter step's `id`. Inner step ids and inner trace entries do not appear in the outer `ExecutionTrace`. Resilience policies attached to the adapter step wrap the entire inner pipeline invocation. This is the primary pattern for scatter-gather (a multi-step inner pipeline used inside `.foreach()`) and for composing pre-built pipelines as reusable building blocks.
- Step output auto-materialization: `StepDescriptor.withOutputKey(StateKey<O> key)` declares that a step's validated output should be automatically written to the given state key after every successful execution — no `ctx.state().set(...)` boilerplate inside `execute()`. Works for sequential steps, parallel branch steps (both combiner and combiner-free variants), and foreach element steps. The key is preserved across `withRetry(...)`, `withTimeout(...)`, and `withCircuitBreaker(...)` builder calls. Output is only written on success — a failing step never mutates state.

## What FlowPipe is

A Java library for orchestrating synchronous API pipelines. Developers define reusable typed steps and compose them into pipelines that execute sequentially, in parallel, or conditionally. The goal is to remove the boilerplate of validation, downstream fan-out, conditional routing, retries, logging, and metrics from every REST handler — so adding a new API flow is a matter of wiring steps, not rebuilding plumbing.

It is **embeddable** — no servers, no runtimes, no daemons. It must run unchanged on AWS Lambda and EC2.

## Design reference: Mastra workflows

FlowPipe's design is heavily influenced by Mastra (`packages/core/src/workflows` in `mastra-ai/mastra`). When extending the design, read the corresponding Mastra concept first — it is the closest existing prior art. The Java translation diverges in a few places, deliberately:

- **Three execution contexts** per step (Mastra has two):
  1. Typed step input/output — the primary data channel, schema-validated before/after each step.
  2. Mutable shared state — execution-scoped, readable/writable by any step.
  3. Immutable request context — tenant id, trace id, etc.; read-only.
- **`pipeline.build()`** is the wiring/validation seam (Mastra's `.commit()`). Schema mismatches, branch-arity errors, and other wiring bugs must surface here, before the first request runs.
- **Branch outputs share a single concrete type** rather than being keyed by branch id with optional fields — preserves Java's compile-time type safety.
- **No streaming, no suspend/resume, no durable execution.** Mastra's `evented/`, `scheduler/`, and persistence machinery have no analog here.

## Hard constraints

- **No Spring dependency in core.** A Spring integration may exist as a separate optional module later, but the core library must be usable without it.
- **No external runtime assumptions.** Code that only works on a long-lived JVM (e.g. relies on background threads outliving a request) breaks the Lambda use case.
- **Step authors write zero logging code and zero metrics code.** Both are framework concerns, injected around every step. If a feature requires step authors to opt into instrumentation, it's the wrong design.
- **Input validated before each step, output validated after.** No "trust the previous step" shortcuts.
- **Retries are configurable but invisible to step authors** — the `execute` method sees one call, the framework handles backoff.
- **Timeouts are configurable but invisible to step authors** — the `execute` method sees one call per attempt, the framework enforces the deadline and surfaces a `StepTimeoutException` on breach.

## Explicit non-goals

These should not be implemented and should not creep in via "while we're at it" PRs:

- YAML / JSON flow definitions (pipelines are Java code).
- Agents, LLMs, or any AI features.
- Durable execution, database persistence, suspend/resume.
- Background job processing.
- Visual workflow editor.

## Module and package layout (current)

- Gradle (Kotlin DSL) multi-module build, Java 21 toolchain, `-Xlint:all -Werror`.
- `flowpipe-core` — the library itself. Runtime dependencies: `slf4j-api`, `dev.failsafe:failsafe:3.3.2` (resilience engine — retry, timeout, circuit breaker; kept as a private engine detail, never exposed in the public API).
  - `io.flowpipe.api` — public surface: `Step`, `StepDescriptor` (includes `outputKey()` / `withOutputKey(StateKey<O>)`), `StepContext`, `Result`, `Success`, `Failure`, `ExecutionTrace`, `TraceEntry`, `TriFunction`, `QuadFunction`, `RetryPolicy`, `PipelineLifecycle<I,O>`, `TimeoutPolicy`, `StepTimeoutException`, `CircuitBreakerPolicy`, `CircuitBreakerOpenException`, `PipelineDescriptor`, `NodeDescriptor`, `PipelineDeadlineExceededException`.
  - `io.flowpipe.state` — `State`, `StateKey<T>`, `RequestContext`, `ContextKey<T>`. `State` is backed by `ConcurrentHashMap` and is safe for concurrent reads/writes (e.g. during `foreach` with `concurrency > 1`); compound read-modify-write operations are still the caller's responsibility. `StateKey` and `ContextKey` equality is based on **both name and type** — two keys with the same name but different types are distinct entries in the map.
  - `io.flowpipe.validation` — `Validator<T>` SPI, `NoOpValidator`, `ValidationException`.
  - `io.flowpipe.observability` — `MetricsRecorder` SPI, `NoOpMetricsRecorder` default, `SpanRecorder` SPI, `NoOpSpanRecorder` default, `StepOutcome` enum. Logger plumbing (SLF4J `step.start` / `step.finish` / `step.error` / `step.retry` / `step.circuit_open` emission) lives inside `io.flowpipe.engine.Pipeline`. `SpanRecorder` fires `startStep`/`finishStep` around every step and every branch node (including skipped ones at any nesting depth); exceptions from the recorder are suppressed.
  - `io.flowpipe.engine` — `Pipeline`, `PipelineBuilder` (`.withMetrics(...)`, `.withTracing(...)`, `.withExecutor(...)`, `.withLifecycle(...)`, `.withDeadline(...)`, `.parallel2/3/4/N(...)`, `.branch(...)`, `.foreach(...)`), `PipelineBuildException`, internal `DefaultStepContext`, internal `FailsafePolicies` (translates FlowPipe policy types to Failsafe 3.x objects; never exposed beyond this package). Internal engine node types `EngineNode`, `StepNode`, `ParallelNode`, `BranchNode`, and `ForeachNode` are package-private sealed types not visible to callers.
- `flowpipe-test` — test utilities (`StepHarness`, `Steps`, `RecordingMetricsRecorder`). Depends on `flowpipe-core` and re-exports JUnit 5 + AssertJ for downstream consumers.
- Future optional modules (e.g. `flowpipe-spring`, `flowpipe-micrometer`) will live outside core and depend on it, never the reverse.

## Build commands

```
./gradlew build                                # compile + test everything
./gradlew :flowpipe-core:test                  # core tests only
./gradlew :flowpipe-core:test --tests <FQN>    # a specific test class or method
./gradlew clean build                          # from a clean state
```

## Working agreements for Claude sessions

- Before designing or extending a feature, check the corresponding Mastra concept (overview, control-flow, workflow-state, error-handling docs). Note deliberately where FlowPipe diverges and why.
- Do not introduce dependencies into `flowpipe-core` without explicit approval — every dependency is a tax on every consumer. Approved runtime deps are `slf4j-api` and `dev.failsafe:failsafe:3.3.2`; nothing else.
- Build-time guarantees beat runtime checks. If a wiring mistake can be caught in `pipeline.build()`, it must be.
