# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

All planned features have landed. The current feature set:
- Gradle scaffold, `flowpipe-core`, `flowpipe-test`
- Core type surface: `Step`, `Pipeline`, `Result`, `State`, `RequestContext`
- Sequential composition with build-time wiring validation
- Observability emission: SLF4J structured logs (`step.start` / `step.finish` / `step.error`) and `MetricsRecorder` SPI around every step
- Parallel composition: `parallel2`/`parallel3`/`parallel4` (typed combiners), `parallelN` (variadic), `withExecutor(ExecutorService)`
- Conditional branching: `branch(branchId, predicate, ifTrue, ifFalse)` with build-time type checking on arm inputs/outputs
- Retry with backoff: `RetryPolicy` on `StepDescriptor`, configurable max attempts, initial delay, multiplier, and jitter
- Lifecycle hooks: `PipelineLifecycle<I, O>` SPI registered via `.withLifecycle(...)` on `PipelineBuilder`; `onStart` / `onFinish` / `onError` callbacks fire at the top-level pipeline boundary only
- Foreach fan-out: `.foreach(step)` / `.foreach(step, concurrency)` on `PipelineBuilder`; accepts a `List<E>` input and applies the step to each element, collecting `List<R>` output; concurrency > 1 runs elements in windowed parallel batches using the pipeline's executor
- Per-step execution timeout: `TimeoutPolicy` on `StepDescriptor` via `.withTimeout(...)`; `TimeoutPolicy.ofMillis(ms)` / `TimeoutPolicy.of(duration, unit)`; exceeded deadline surfaces as `Failure` with `StepTimeoutException` as the cause; each retry attempt receives its own independent deadline
- Circuit breaker policy: `CircuitBreakerPolicy` on `StepDescriptor` via `.withCircuitBreaker(...)`; CLOSED/OPEN/HALF-OPEN state machine with configurable failure-rate threshold, sliding window, open window, and half-open probe count; OPEN circuit fast-fails with `CircuitBreakerOpenException` before retry or timeout logic runs; state is per-`Pipeline`-instance, keyed by step id, persists across executions; circuit records the final retry-loop outcome (not individual attempt outcomes)

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

- Gradle (Kotlin DSL) multi-module build, Java 17 toolchain, `-Xlint:all -Werror`.
- `flowpipe-core` — the library itself. Runtime dependencies: `slf4j-api`, `dev.failsafe:failsafe:3.3.2` (resilience engine — retry, timeout, circuit breaker; kept as a private engine detail, never exposed in the public API).
  - `io.flowpipe.api` — public surface: `Step`, `StepDescriptor`, `StepContext`, `Result`, `Success`, `Failure`, `ExecutionTrace`, `TraceEntry`, `TriFunction`, `QuadFunction`, `RetryPolicy`, `PipelineLifecycle<I,O>`, `TimeoutPolicy`, `StepTimeoutException`, `CircuitBreakerPolicy`, `CircuitBreakerOpenException`.
  - `io.flowpipe.state` — `State`, `StateKey<T>`, `RequestContext`, `ContextKey<T>`.
  - `io.flowpipe.validation` — `Validator<T>` SPI, `NoOpValidator`, `ValidationException`.
  - `io.flowpipe.observability` — `MetricsRecorder` SPI, `NoOpMetricsRecorder` default, `StepOutcome` enum. Logger plumbing (SLF4J `step.start` / `step.finish` / `step.error` / `step.retry` / `step.circuit_open` emission) lives inside `io.flowpipe.engine.Pipeline`.
  - `io.flowpipe.engine` — `Pipeline`, `PipelineBuilder` (`.withMetrics(...)`, `.withExecutor(...)`, `.withLifecycle(...)`, `.parallel2/3/4/N(...)`, `.branch(...)`, `.foreach(...)`), `PipelineBuildException`, internal `DefaultStepContext`, internal `FailsafePolicies` (translates FlowPipe policy types to Failsafe 3.x objects; never exposed beyond this package). Internal engine node types `EngineNode`, `StepNode`, `ParallelNode`, `BranchNode`, and `ForeachNode` are package-private sealed types not visible to callers.
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
