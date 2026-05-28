# FlowPipe

A Java library for orchestrating synchronous API pipelines. Compose typed, reusable steps into pipelines with automatic input/output validation, build-time wiring checks, and zero-boilerplate logging and metrics around every step.

## Requirements

- Java 21 or newer
- Gradle (the repo ships a wrapper at `./gradlew`)

## Build and test

```
./gradlew build
```

## Example

Define two steps, chain them, execute, then discriminate the result.

```java
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;

public class Example {

    public static void main(String[] args) {
        Step<String, Integer> parse = Step.of(
            "parse", String.class, Integer.class,
            (input, ctx) -> Integer.parseInt(input));

        Step<Integer, String> describe = Step.of(
            "describe", Integer.class, String.class,
            (input, ctx) -> input >= 0 ? "non-negative" : "negative");

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(parse)
            .then(describe)
            .build();

        Result<String> result = pipeline.execute("42");

        if (result instanceof Success<String> s) {
            System.out.println("ok: " + s.value());
        } else if (result instanceof Failure<String> f) {
            System.err.println("failed at " + f.failedStepId() + ": " + f.cause());
        }
    }
}
```

`pipeline.build()` validates wiring before any request runs: empty pipelines, duplicate step ids, and step-to-step type mismatches all fail at build time, not execution time.

## Parallel composition

Fan out to multiple independent steps simultaneously and merge their typed outputs with a combiner:

```java
import io.flowpipe.api.Step;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;

Step<String, Integer> wordCount = Step.of(
    "wordCount", String.class, Integer.class,
    (text, ctx) -> text.split("\\s+").length);

Step<String, Integer> charCount = Step.of(
    "charCount", String.class, Integer.class,
    (text, ctx) -> text.length());

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .parallel2(
        String.class,
        (words, chars) -> "words=" + words + " chars=" + chars,
        wordCount,
        charCount)
    .build();
```

Both branches receive the same input concurrently. The combiner's return type becomes the next step's input type. `parallel3` and `parallel4` extend the pattern to three and four branches; `parallelN` handles higher arities with an untyped `Map`-based combiner.

### Executor

By default, branches are submitted to `ForkJoinPool.commonPool()`. Supply a dedicated executor with `.withExecutor(ExecutorService)`:

```java
Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .parallel2(String.class, (a, b) -> a + b, stepA, stepB)
    .withExecutor(Executors.newFixedThreadPool(4))
    .build();
```

**Lambda users**: on a 1-vCPU Lambda the common pool's parallelism is 0, which means branches execute on the calling thread and parallelism is illusory. Supply a `Executors.newCachedThreadPool()` executor (and shut it down after the invocation) to get real concurrency. FlowPipe never shuts down the executor it is given.

### Resilience policies on parallel branches

`RetryPolicy`, `TimeoutPolicy`, and `CircuitBreakerPolicy` attached to a parallel branch step's `StepDescriptor` are fully honored — the branch runs through the same retry/timeout/circuit-breaker machinery as sequential steps. A retry configured on a parallel branch retries within that branch's executor thread, transparent to the rest of the pipeline.

## Pipeline composition

`Pipeline.asStep(String id)` turns any `Pipeline<I,O>` into a `Step<I,O>`, enabling pipelines to be nested inside other pipelines anywhere a step is accepted — `.then()`, `.foreach()`, `.parallel2/3/4/N()`, and `.branch()`.

### Basic nesting with `.then()`

```java
// Inner pipeline: parse a string, then double the integer
Step<String, Integer> parse = Step.of("parse", String.class, Integer.class,
    (s, ctx) -> Integer.parseInt(s));
Step<Integer, Integer> doubler = Step.of("double", Integer.class, Integer.class,
    (n, ctx) -> n * 2);

Pipeline<String, Integer> inner = PipelineBuilder.start(String.class)
    .then(parse)
    .then(doubler)
    .build();

// Outer pipeline: run inner, then add 10
Pipeline<String, Integer> outer = PipelineBuilder.start(String.class)
    .then(inner.asStep("transform"))   // "transform" is the step id as seen by the outer pipeline
    .then(addTenStep)
    .build();

// execute("5") → inner produces 10, outer produces 20
```

The string passed to `asStep()` is the step id the outer pipeline uses for logging, metrics, and the `ExecutionTrace`. Inner step ids never appear in the outer trace — each pipeline maintains its own trace.

### Scatter-gather with `.foreach()`

The primary Mastra pattern for multi-step fan-out: apply a full sub-pipeline to every element of a list.

```java
// Inner pipeline: trim whitespace, then uppercase
Pipeline<String, String> enrich = PipelineBuilder.start(String.class)
    .then(trimStep)
    .then(upperStep)
    .build();

// Outer pipeline: split input into a list, then enrich each element
Pipeline<String, List<String>> outer = PipelineBuilder.start(String.class)
    .then(splitStep)                  // Step<String, List<String>>
    .foreach(enrich.asStep("enrich")) // applies the two-step inner pipeline per element
    .build();

// execute(" hello , world ") → ["HELLO", "WORLD"]
```

`foreach(step, N)` with `N > 1` runs elements concurrently in windows of `N`, using the pipeline's executor.

### Parallel branches

```java
// Two inner pipelines running concurrently on the same input
Pipeline<String, String> wordStats = PipelineBuilder.start(String.class)
    .then(wordCountStep)
    .then(formatStep)
    .build();

Pipeline<String, String> outer = PipelineBuilder.start(String.class)
    .parallel2(
        String.class,
        (wordResult, charCount) -> wordResult + " chars=" + charCount,
        wordStats.asStep("word-stats"),  // runs a full inner pipeline as a branch
        charCountStep)
    .build();
```

### Failure propagation

When the inner pipeline fails, the original exception becomes `Failure.cause()` in the outer pipeline. The `failedStepId()` is the adapter step's id (e.g. `"transform"`), not the id of the inner step that actually threw. This keeps the outer pipeline's failure reporting stable regardless of what's inside the inner pipeline.

```java
Result<Integer> result = outer.execute("not-a-number");
if (result instanceof Failure<Integer> f) {
    f.failedStepId(); // "transform" — the adapter step id
    f.cause();        // NumberFormatException from the inner parse step
}
```

### Context propagation and state isolation

The outer pipeline's `RequestContext` (tenant id, trace id, etc.) is forwarded into the inner pipeline automatically. Inner `State` is isolated — each inner pipeline execution creates its own `State`, independent of the outer pipeline's `State`.

### Observability

The outer pipeline emits a single `step.start` / `step.finish` (or `step.error`) pair for the adapter step. The inner pipeline emits its own `step.start` / `step.finish` events for each of its steps. If both pipelines have `MetricsRecorder`s configured, both record metrics independently. Configure the inner pipeline with `.withMetrics(recorder)` at build time to give it its own recorder.

### Resilience on the adapter step

Resilience policies attached to the adapter step wrap the **entire** inner pipeline invocation. A retry retries the whole inner pipeline from the beginning, not an individual inner step.

```java
// Wrap the adapter in a custom StepDescriptor to attach a retry policy
Step<String, String> base = inner.asStep("retriable-sub");
Step<String, String> withRetry = new Step<>() {
    @Override
    public StepDescriptor<String, String> describe() {
        return StepDescriptor.builder("retriable-sub", String.class, String.class)
            .withRetry(RetryPolicy.fixed(3, 50))
            .build();
    }
    @Override
    public String execute(String input, StepContext ctx) throws Exception {
        return base.execute(input, ctx);
    }
};

Pipeline<String, String> outer = PipelineBuilder.start(String.class)
    .then(withRetry)
    .build();
```

## Conditional branching

Route execution down one of two typed sub-pipelines based on a predicate. Both arms must produce the same output type; type mismatches are caught at `build()` time.

```java
Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .branch(
        "check-stock",
        (order, ctx) -> inventoryService.isInStock(order),
        PipelineBuilder.start(Order.class).then(processStep).build(),
        PipelineBuilder.start(Order.class).then(backorderStep).build())
    .build();
```

The skipped arm's steps appear in the `ExecutionTrace` as skipped entries.

## Retry with backoff

Attach a `RetryPolicy` to any `StepDescriptor` to make the framework retry that step transparently on failure. Step authors write no retry code.

```java
StepDescriptor<Order, PaymentResult> desc = StepDescriptor
    .builder("charge-card", Order.class, PaymentResult.class)
    .withRetry(RetryPolicy.exponential(3, 100, 2.0, true))
    .build();
```

`RetryPolicy.fixed(attempts, delayMs)` gives constant-delay retries; `RetryPolicy.exponential(attempts, initialMs, multiplier, jitter)` doubles the delay between attempts. `RetryPolicy.none()` (the default) means one attempt with no retry.

## Lifecycle hooks

Register an `onStart` / `onFinish` / `onError` listener at the pipeline boundary. Useful for distributed tracing spans, audit logs, and top-level error reporting.

```java
Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .then(validateStep)
    .then(chargeStep)
    .withLifecycle(new PipelineLifecycle<Order, OrderResult>() {
        @Override public void onStart(Order input, StepContext ctx) {
            tracer.startSpan(ctx.context().get(TRACE_ID_KEY));
        }
        @Override public void onFinish(Result<OrderResult> result, StepContext ctx) {
            tracer.finishSpan();
        }
        @Override public void onError(Failure<OrderResult> failure, StepContext ctx) {
            alerts.send(failure.failedStepId(), failure.cause());
        }
    })
    .build();
```

Hooks fire at the top-level pipeline boundary only; sub-pipelines inside `branch(...)` do not re-fire the parent hooks.

## Foreach fan-out

Apply a step to every element of a `List` input and collect the results. Optionally run elements concurrently in a fixed-size window.

```java
Step<String, UserProfile> enrichStep = Step.of(
    "enrich-user", String.class, UserProfile.class,
    (userId, ctx) -> profileService.fetch(userId));

Pipeline<List<String>, List<UserProfile>> pipeline = PipelineBuilder
    .start((Class<List<String>>) (Class<?>) List.class)
    .foreach(enrichStep)          // sequential (concurrency = 1)
    // .foreach(enrichStep, 8)    // up to 8 concurrent
    .build();
```

Sequential foreach preserves order; concurrent foreach processes elements in windows of the given size using the pipeline's executor.

## Per-step timeout

Attach a `TimeoutPolicy` to any `StepDescriptor` to give that step a hard deadline per attempt. A step that exceeds its deadline is interrupted and the pipeline produces a `Failure` whose `cause()` is a `StepTimeoutException`.

```java
StepDescriptor<String, UserProfile> desc = StepDescriptor
    .builder("fetch-profile", String.class, UserProfile.class)
    .withTimeout(TimeoutPolicy.ofMillis(500))
    .build();
```

`TimeoutPolicy.of(2, TimeUnit.SECONDS)` is an alternative constructor. When combined with a `RetryPolicy`, each retry attempt gets its own independent deadline. Steps without a `TimeoutPolicy` (the default `TimeoutPolicy.none()`) run without a time bound.

## Pipeline deadline

Set a wall-clock budget for an entire pipeline execution with `.withDeadline(long ms)`. The deadline is checked before every sequential step, before and during parallel block execution, before branch arm execution, and between foreach items.

```java
Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .then(validateStep)
    .then(enrichStep)
    .withDeadline(2_000) // 2-second total budget
    .build();
```

If the budget is exceeded at any point, execution stops immediately and the pipeline returns a `Failure` whose `cause()` is `PipelineDeadlineExceededException` (which carries `deadlineMs()`) and whose `failedStepId()` is `"pipeline.deadline"`.

`TimeoutPolicy.withDeadline(long duration, TimeUnit unit)` is an alternative constructor. The per-step `TimeoutPolicy` and the pipeline deadline are independent: a step can have both, and the tighter bound wins.

## Circuit breaker

Attach a `CircuitBreakerPolicy` to any `StepDescriptor` to prevent cascading failures when a downstream dependency is unhealthy. The circuit transitions through CLOSED → OPEN → HALF-OPEN states automatically.

```java
StepDescriptor<String, UserProfile> desc = StepDescriptor
    .builder("fetch-profile", String.class, UserProfile.class)
    .withCircuitBreaker(CircuitBreakerPolicy.of(
        50,      // open when ≥ 50 % of calls fail
        5,       // minimum calls before the rate is evaluated
        10,      // sliding window size
        30_000L, // stay open for 30 s
        2))      // allow 2 probe calls in HALF-OPEN before closing
    .build();
```

`CircuitBreakerPolicy.defaults()` gives reasonable out-of-the-box settings (50 % threshold, 5 minimum calls, window of 10, 60 s open window, 2 half-open probes). When the circuit is OPEN, the step fast-fails with a `Failure` whose `cause()` is a `CircuitBreakerOpenException` — no call is made to the step's `execute` method. Circuit state is per-`Pipeline` instance, keyed by step id, and persists across `pipeline.execute(...)` calls. When combined with `RetryPolicy`, the circuit evaluates the final outcome of the retry loop, not individual attempt outcomes.

`minimumCalls` sets a hard floor: the circuit will not open until at least that many failures have been observed, even if the failure-rate threshold would otherwise be met sooner.

`CircuitBreakerPolicy` works on sequential steps, foreach steps, and parallel branch steps.

## Observability

Every step invocation emits three structured SLF4J events — `step.start`, then either `step.finish` (success) or `step.error` (failure) — carrying `step.id`, `step.attempt`, `step.duration_ms`, `step.outcome`, error class/message on failure, and every `RequestContext` entry as a structured key-value field. Configure your SLF4J backend the usual way; no library configuration required.

Plug in a metrics backend by implementing `MetricsRecorder`:

```java
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.StepOutcome;

class MicrometerRecorder implements MetricsRecorder {
    private final io.micrometer.core.instrument.MeterRegistry registry;
    MicrometerRecorder(io.micrometer.core.instrument.MeterRegistry r) { this.registry = r; }

    @Override public void recordStepDuration(String stepId, long durationNanos) {
        registry.timer("flowpipe.step.duration", "step", stepId)
            .record(durationNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }
    @Override public void recordStepAttempts(String stepId, int attempts) {
        registry.counter("flowpipe.step.attempts", "step", stepId).increment(attempts);
    }
    @Override public void recordStepOutcome(String stepId, StepOutcome outcome) {
        registry.counter("flowpipe.step.outcome", "step", stepId, "outcome", outcome.name())
            .increment();
    }
}

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .then(parse).then(describe)
    .withMetrics(new MicrometerRecorder(registry))
    .build();
```

Need a per-call override (e.g., in a test)? Use the three-argument `execute(input, context, recorder)` overload. Recorder exceptions are caught and logged as `metrics.recorder_failed` — they never affect the pipeline result.

## Project layout

- `flowpipe-core` — library
- `flowpipe-test` — test utilities (`StepHarness`, factory `Steps`, `RecordingMetricsRecorder`)
- `openspec/changes/` — proposals, designs, and specs for upcoming work
