# FlowPipe

A Java library for orchestrating synchronous API pipelines. Compose typed, reusable steps into pipelines with automatic input/output validation, build-time wiring checks, and zero-boilerplate logging and metrics around every step.

FlowPipe is **embeddable** — no servers, no runtimes, no daemons. It runs unchanged on AWS Lambda and EC2.

## Requirements

- Java 21 or newer
- Gradle (the repo ships a wrapper at `./gradlew`)

## Build and test

```
./gradlew build
```

## Quick start

Define two steps, chain them, execute, then discriminate the result.

```java
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;

Step<String, Integer> parse = Step.of(
    "parse", String.class, Integer.class,
    (input, ctx) -> Integer.parseInt(input));

Step<Integer, String> describe = Step.of(
    "describe", Integer.class, String.class,
    (input, ctx) -> input >= 0 ? "non-negative" : "negative");

Pipeline<String, String> pipeline = Pipeline.builder(String.class)
    .then(parse)
    .then(describe)
    .build();

Result<String> result = pipeline.execute("42");

if (result instanceof Success<String> s) {
    System.out.println("ok: " + s.value());
} else if (result instanceof Failure<String> f) {
    System.err.println("failed at " + f.failedStepId() + ": " + f.cause());
}
```

`pipeline.build()` validates wiring before any request runs: empty pipelines, duplicate step ids, and step-to-step type mismatches all fail at build time, not at execution time.

---

## Defining steps

### Inline steps with `Step.of()`

For simple, one-off steps, use the factory:

```java
Step<String, Integer> parse = Step.of(
    "parse",           // unique id within the pipeline
    String.class,      // input type
    Integer.class,     // output type
    (input, ctx) -> Integer.parseInt(input));
```

The id is used in logs, metrics, trace entries, and failure reporting. It must be unique within the pipeline — `build()` will reject duplicates.

### Steps with policies using `Step.builder()`

When you need retry, timeout, circuit breaker, or validation policies, use the fluent builder — all configuration lives in one expression:

```java
Step<String, UserProfile> fetchProfile = Step.builder("fetch-profile", String.class, UserProfile.class)
    .execute((userId, ctx) -> profileService.fetch(userId))
    .withRetry(RetryPolicy.exponential(3, 100, 2.0, true))
    .withTimeout(TimeoutPolicy.ofMillis(500))
    .withCircuitBreaker(CircuitBreakerPolicy.defaults())
    .withInputValidator(input -> {
        if (input == null || input.isBlank())
            throw new ValidationException("userId must not be blank");
    })
    .build();
```

The `.execute()` body accepts checked exceptions directly — no wrapping required. All policies are applied transparently by the framework; the body sees a single call per attempt and writes no retry or timeout code.

A step returning `null` from `execute()` immediately surfaces as `Failure` with a `NullPointerException` naming the offending step, before any output validation runs.

### Reusable step classes

For library steps shared across multiple pipelines, implement `Step<I,O>` directly and return a `StepDescriptor` from `describe()`:

```java
public class FetchProfileStep implements Step<String, UserProfile> {
    private final ProfileService profileService;

    public FetchProfileStep(ProfileService profileService) {
        this.profileService = profileService;
    }

    @Override
    public StepDescriptor<String, UserProfile> describe() {
        return StepDescriptor.builder("fetch-profile", String.class, UserProfile.class)
            .withRetry(RetryPolicy.fixed(3, 100))
            .build();
    }

    @Override
    public UserProfile execute(String userId, StepContext ctx) throws Exception {
        return profileService.fetch(userId);
    }
}
```

Use this pattern for steps in shared libraries (like `flowpipe-commons`) or when the step needs constructor-injected dependencies that vary per caller.

### Auto-writing step output to state with `withOutputKey`

Declare `withOutputKey(StateKey<O>)` and the framework automatically writes the step's validated output to the given state key after every successful execution — no `ctx.state().set(...)` inside `execute()`:

```java
StateKey<UserProfile> PROFILE_KEY = StateKey.of("profile", UserProfile.class);

Step<String, UserProfile> fetchProfile = Step.builder("fetch-profile", String.class, UserProfile.class)
    .execute((userId, ctx) -> profileService.fetch(userId))  // no state.set() here
    .withRetry(RetryPolicy.fixed(3, 100))
    .withOutputKey(PROFILE_KEY)  // framework writes output to state after execution
    .build();

// Any downstream step reads from state directly
Step<Order, OrderResult> charge = Step.of("charge", Order.class, OrderResult.class,
    (order, ctx) -> {
        UserProfile profile = ctx.state().get(PROFILE_KEY);  // just works
        return paymentService.charge(order, profile);
    });
```

The key is preserved across `withRetry(...)`, `withTimeout(...)`, and `withCircuitBreaker(...)` calls. Output is only written on success — a failing step never mutates state. Works for sequential steps, parallel branch steps, and foreach element steps.

### Accessing shared state and request context

The `StepContext` passed to every `execute()` call provides two data channels:

**`State`** — mutable, execution-scoped, shared across all steps in a single `pipeline.execute(...)` call. Backed by `ConcurrentHashMap`, safe for concurrent reads/writes during parallel or concurrent-foreach execution. Compound read-modify-write operations are the caller's responsibility.

```java
// Define typed keys — equality is based on both name and type
StateKey<List<String>> errorsKey = StateKey.of("validation-errors", List.class);

// A step that writes to state
Step<Order, Order> validate = Step.of("validate", Order.class, Order.class,
    (order, ctx) -> {
        List<String> errors = new ArrayList<>();
        if (order.amount() <= 0) errors.add("amount must be positive");
        ctx.state().set(errorsKey, errors);
        return order;
    });

// A later step that reads from state
Step<Order, OrderResult> charge = Step.of("charge", Order.class, OrderResult.class,
    (order, ctx) -> {
        List<String> errors = ctx.state().get(errorsKey);
        if (errors != null && !errors.isEmpty()) throw new ValidationException(errors.toString());
        return paymentService.charge(order);
    });
```

**`RequestContext`** — immutable, request-scoped, set before the pipeline executes. Used for tenant IDs, trace IDs, feature flags, or any per-request metadata. Steps only read it; they cannot write to it.

```java
// Define typed keys
ContextKey<String> tenantKey = ContextKey.of("tenant-id", String.class);
ContextKey<String> traceKey  = ContextKey.of("trace-id",  String.class);

// Build and pass context at execution time
RequestContext ctx = RequestContext.builder()
    .put(tenantKey, "acme-corp")
    .put(traceKey,  "abc-123")
    .build();

Result<String> result = pipeline.execute(input, ctx);

// Inside a step
Step<Order, Order> auditStep = Step.of("audit", Order.class, Order.class,
    (order, stepCtx) -> {
        String tenant = stepCtx.context().get(tenantKey); // "acme-corp"
        auditLog.record(tenant, order);
        return order;
    });
```

Every `RequestContext` entry is automatically included as a structured key-value field in every SLF4J log event emitted by the framework.

---

## Composing pipelines

### Sequential composition

Chain steps with `.then()`. Each step's output type must match the next step's input type — verified at `build()` time:

```java
Pipeline<String, OrderResult> pipeline = PipelineBuilder.start(String.class)
    .then(parseStep)      // Step<String, Order>
    .then(validateStep)   // Step<Order, Order>
    .then(chargeStep)     // Step<Order, OrderResult>
    .build();
```

The builder enforces type compatibility at each `.then()` call and again in `build()`. A mismatch throws `PipelineBuildException` immediately — no request needs to run for the bug to be caught.

### Executing a pipeline

Three overloads are available:

```java
// No context, no metrics override
Result<OrderResult> r1 = pipeline.execute(input);

// With request context (tenant id, trace id, etc.)
Result<OrderResult> r2 = pipeline.execute(input, requestContext);

// With context and a per-call metrics recorder override
Result<OrderResult> r3 = pipeline.execute(input, requestContext, metricsRecorder);
```

Passing `null` as `input` throws `NullPointerException` immediately, before lifecycle hooks fire or any step runs.

### The Result type

`Result<O>` is a sealed interface with exactly two implementations:

```java
if (result instanceof Success<OrderResult> s) {
    OrderResult value = s.value();
    ExecutionTrace trace = s.trace();  // one TraceEntry per step
}
if (result instanceof Failure<OrderResult> f) {
    String stepId   = f.failedStepId(); // id of the step that threw
    Throwable cause = f.cause();        // the original exception, never wrapped
    ExecutionTrace trace = f.trace();   // entries for steps that ran before the failure
}
```

`ExecutionTrace.entries()` returns one `TraceEntry` per step: step id, start timestamp (nanoseconds), duration (nanoseconds), attempt count, and a `skipped` flag for branch arms that did not run.

---

## Parallel composition

Fan out to multiple independent steps simultaneously and merge their typed outputs with a combiner.

### `parallel2` / `parallel3` / `parallel4`

```java
Step<String, Integer> wordCount = Step.of("wordCount", String.class, Integer.class,
    (text, ctx) -> text.split("\\s+").length);

Step<String, Integer> charCount = Step.of("charCount", String.class, Integer.class,
    (text, ctx) -> text.length());

Step<String, Long> lineCount = Step.of("lineCount", String.class, Long.class,
    (text, ctx) -> text.lines().count());

// parallel2 — two branches, typed combiner
Pipeline<String, String> two = PipelineBuilder.start(String.class)
    .parallel2(
        String.class,                                          // result type
        (words, chars) -> "words=" + words + " chars=" + chars, // BiFunction<A, B, R>
        wordCount,
        charCount)
    .build();

// parallel3 — three branches
Pipeline<String, String> three = PipelineBuilder.start(String.class)
    .parallel3(
        String.class,
        (words, chars, lines) -> words + " / " + chars + " / " + lines, // TriFunction<A, B, C, R>
        wordCount,
        charCount,
        lineCount)
    .build();
```

`Class<R>` is the first argument and enables downstream type-compatibility checking at `build()` time. `parallel4` follows the same pattern with a `QuadFunction<A, B, C, D, R>` combiner.

All branches receive the same input (the current pipeline cursor value). The combiner is called with branch outputs **in declaration order**, not completion order. No subsequent step runs until all branches have either completed or failed.

A combiner returning `null` surfaces immediately as `Failure` with `failedStepId="parallel.combiner"`, consistent with the rule that step and combiner outputs must not be null.

### `parallelN` — variadic escape hatch for arities above 4

```java
Map<String, Step<String, ?>> steps = new LinkedHashMap<>();
steps.put("words", wordCount);   // key must match step.describe().id()
steps.put("chars", charCount);
steps.put("lines", lineCount);

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .parallelN(
        String.class,
        steps,
        results -> {            // Function<Map<String, Object>, R>
            int words = (int)  results.get("words");
            int chars = (int)  results.get("chars");
            long lines = (long) results.get("lines");
            return words + " words, " + chars + " chars, " + lines + " lines";
        })
    .build();
```

The map keys must match the corresponding step's `StepDescriptor.id()`. A mismatch is caught at `build()` and throws `PipelineBuildException`.

### Combiner-free parallel — outputs go to state, no holder type needed

When branches don't need to be merged into a typed result, use the combiner-free overloads: `parallel2(stepA, stepB)`, `parallel3`, `parallel4`, or `parallelN(List<Step<O,?>>)`. Each branch must declare `withOutputKey(...)` on its `StepDescriptor` — the framework writes each branch's output to state after execution, and the current pipeline value passes through unchanged.

```java
StateKey<List<TextResult>>  TEXT_KEY  = StateKey.of("text-results",  List.class);
StateKey<List<ImageResult>> IMAGE_KEY = StateKey.of("image-results", List.class);

Step<String, List<TextResult>> textSearch = new Step<>() {
    StepDescriptor<String, List<TextResult>> desc = StepDescriptor
        .builder("text-search", String.class, List.class)
        .withOutputKey(TEXT_KEY)   // ← required for combiner-free parallel
        .build();
    @Override public StepDescriptor<String, List<TextResult>> describe() { return desc; }
    @Override public List<TextResult> execute(String query, StepContext ctx) {
        return searchService.searchText(query);
    }
};

// imageSearch declared similarly with .withOutputKey(IMAGE_KEY)

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .parallel2(textSearch, imageSearch)   // no Class<R> or combiner — input passes through
    .then(Step.of("assemble", String.class, String.class, (query, ctx) -> {
        List<TextResult>  text   = ctx.state().get(TEXT_KEY);   // results in state
        List<ImageResult> images = ctx.state().get(IMAGE_KEY);
        return renderResults(query, text, images);
    }))
    .build();
```

`build()` enforces that every branch in a combiner-free block declares `withOutputKey`. If any branch is missing it, `build()` throws `PipelineBuildException` naming the offending branch(es).

Combiner-based and combiner-free blocks can be freely mixed in the same pipeline.

### Executor

By default, branches are submitted to `ForkJoinPool.commonPool()`. Supply a dedicated executor:

```java
Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .parallel2(String.class, (a, b) -> a + b, stepA, stepB)
    .withExecutor(Executors.newFixedThreadPool(4))
    .build();
```

FlowPipe **never** shuts down the executor it is given — lifecycle is the caller's responsibility.

**Lambda users**: on a 1-vCPU Lambda the common pool's parallelism is 0, so branches execute on the calling thread and parallelism is illusory. Supply `Executors.newCachedThreadPool()` and shut it down after the invocation.

### Resilience policies on parallel branches

`RetryPolicy`, `TimeoutPolicy`, and `CircuitBreakerPolicy` attached to a parallel branch step's `StepDescriptor` are fully honored — the branch runs through the same retry/timeout/circuit-breaker machinery as sequential steps. A retry configured on a parallel branch retries within that branch's executor thread, transparent to the rest of the pipeline.

---

## Conditional branching

Route execution down one of two typed sub-pipelines based on a predicate. Both arms must accept the same input type and produce the same output type — verified at `build()` time.

```java
Pipeline<Order, OrderResult> inStock = PipelineBuilder.start(Order.class)
    .then(processStep)
    .build();

Pipeline<Order, OrderResult> outOfStock = PipelineBuilder.start(Order.class)
    .then(backorderStep)
    .build();

Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .branch(
        "check-stock",                                        // branch id (unique within the pipeline)
        (order, ctx) -> inventoryService.isInStock(order),   // BiPredicate<O, StepContext>
        inStock,                                              // ifTrue arm
        outOfStock)                                           // ifFalse arm
    .build();
```

The skipped arm's steps appear in the `ExecutionTrace` as skipped entries. The predicate receives the full `StepContext` so it can read `State` or `RequestContext` if needed.

### Single-armed branch — optional transformation

When you only need "do something if condition is true; otherwise pass through unchanged," use the three-argument overload that omits the false arm:

```java
Pipeline<Order, Order> premiumPipeline = PipelineBuilder.start(Order.class)
    .then(applyDiscountStep)
    .build();

Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .branch(
        "premium-discount",
        (order, ctx) -> order.isPremiumCustomer(),  // BiPredicate<O, StepContext>
        premiumPipeline)                             // ifTrue arm — Pipeline<O, O>
    // no ifFalse needed — non-premium orders pass through unchanged
    .then(chargeStep)
    .build();
```

Because the false arm is an implicit identity pass-through, the `ifTrue` pipeline must return the same type it receives (`Pipeline<O, O>`). The output type of the builder stays `O` — subsequent steps see no type change regardless of which path ran.

---

## Foreach fan-out

Apply a step to every element of a `List` input and collect the results into a `List`.

```java
Step<String, UserProfile> enrich = Step.of(
    "enrich-user", String.class, UserProfile.class,
    (userId, ctx) -> profileService.fetch(userId));

Pipeline<List<String>, List<UserProfile>> pipeline = PipelineBuilder
    .start((Class<List<String>>) (Class<?>) List.class)
    .foreach(enrich)       // sequential, concurrency = 1
    // .foreach(enrich, 8) // up to 8 concurrent element executions
    .build();
```

Sequential foreach preserves element order. Concurrent foreach (`concurrency > 1`) processes elements in windows of that size using the pipeline's executor; output order matches input order regardless of completion order within a window.

For multi-step processing per element, wrap the logic in a single `Step` implementation that calls each sub-step directly.

---

## Retry with backoff

Attach a `RetryPolicy` to any `StepDescriptor`. The framework retries transparently — step authors write no retry code.

```java
StepDescriptor<Order, PaymentResult> desc = StepDescriptor
    .builder("charge-card", Order.class, PaymentResult.class)
    .withRetry(RetryPolicy.exponential(3, 100, 2.0, true))
    //                     attempts  initial(ms) multiplier jitter
    .build();
```

| Factory | Behaviour |
|---|---|
| `RetryPolicy.none()` | One attempt, no retry (default) |
| `RetryPolicy.fixed(attempts, delayMs)` | Constant delay between attempts |
| `RetryPolicy.exponential(attempts, initialMs, multiplier, jitter)` | Delay grows by `multiplier` each attempt; `jitter=true` adds randomness to avoid thundering herd |

### Selective retry with `.retryOn(Predicate<Throwable>)`

By default a `RetryPolicy` retries on any exception. Use `.retryOn(predicate)` to restrict retries to exceptions that match a condition — non-matching exceptions propagate immediately without consuming any remaining attempts.

```java
import java.io.IOException;
import java.net.SocketTimeoutException;

RetryPolicy policy = RetryPolicy.exponential(3, 100, 2.0, false)
    .retryOn(e -> e instanceof IOException);  // only retry I/O failures

// Combine multiple types:
RetryPolicy policy2 = RetryPolicy.fixed(3, 50)
    .retryOn(e -> e instanceof IOException || e instanceof SocketTimeoutException);

// Exclude specific subtypes:
RetryPolicy policy3 = RetryPolicy.fixed(3, 0)
    .retryOn(e -> e instanceof IOException && !(e instanceof java.net.UnknownHostException));
```

When the predicate returns `false`, the exception propagates immediately as `Failure.cause()` — no further attempts are made and the step is not retried. When it returns `true`, normal retry-with-backoff logic applies.

`.retryOn()` returns a new `RetryPolicy` instance; the original is unchanged (immutable).

```java
RetryPolicy base    = RetryPolicy.fixed(3, 100);  // retries on any exception
RetryPolicy ioOnly  = base.retryOn(e -> e instanceof IOException);  // new instance
// base is still "retry on any exception"
```

A `step.retry` log event is emitted at `WARN` level before each retry attempt, carrying `step.id`, `step.attempt`, `step.max_attempts`, and `step.delay_ms`. A `step.error` log event is emitted for every failed attempt — including attempts where the predicate rejected the exception (so observability is never silently dropped). Metrics are recorded once for the final outcome, not per attempt.

---

## Per-step timeout

Attach a `TimeoutPolicy` to any `StepDescriptor` to give that step a hard deadline per attempt.

```java
StepDescriptor<String, UserProfile> desc = StepDescriptor
    .builder("fetch-profile", String.class, UserProfile.class)
    .withTimeout(TimeoutPolicy.ofMillis(500))
    .build();
```

`TimeoutPolicy.of(2, TimeUnit.SECONDS)` is an alternative constructor. When combined with `RetryPolicy`, each retry attempt gets its own independent deadline — the timeout is per-attempt, not per-retry-loop. A step that exceeds its deadline is interrupted and the pipeline produces a `Failure` whose `cause()` is `StepTimeoutException` (carrying the step id and configured timeout milliseconds). Steps without a `TimeoutPolicy` (the default) run without a time bound.

---

## Pipeline deadline

Set a wall-clock budget for an entire pipeline execution with `.withDeadline(long ms)`. The deadline is checked before every node — sequential steps, parallel blocks, branch arm entry, and between foreach items — and is also enforced while waiting for parallel branch futures (a slow branch cannot hold the pipeline past the deadline).

```java
Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .then(validateStep)
    .then(enrichStep)
    .withDeadline(2_000)                   // 2 000 ms wall-clock budget
    // .withDeadline(2, TimeUnit.SECONDS)  // same, using TimeUnit overload
    .build();
```

If the budget is exceeded, execution stops immediately and the pipeline returns a `Failure` whose `cause()` is `PipelineDeadlineExceededException` (which carries `deadlineMs()`) and whose `failedStepId()` is `"pipeline.deadline"`.

The deadline is enforced everywhere futures are awaited: parallel branch futures and concurrent `foreach` item futures. A slow parallel branch or a slow concurrent foreach window cannot hold the pipeline past the deadline.

The per-step `TimeoutPolicy` and the pipeline deadline are orthogonal: a step can have both. The tighter bound wins — whichever fires first terminates that step. The pipeline deadline propagates automatically into branch arm sub-pipelines.

---

## Circuit breaker

Attach a `CircuitBreakerPolicy` to any `StepDescriptor` to prevent cascading failures when a downstream dependency is unhealthy. The circuit transitions through CLOSED → OPEN → HALF-OPEN states automatically.

```java
StepDescriptor<String, UserProfile> desc = StepDescriptor
    .builder("fetch-profile", String.class, UserProfile.class)
    .withCircuitBreaker(CircuitBreakerPolicy.of(
        50,       // open when ≥ 50 % of calls in the window fail
        5,        // minimumCalls: circuit will not open until at least 5 failures observed
        10,       // sliding window size (last N calls evaluated)
        30_000L,  // stay open for 30 s before allowing a probe call
        2))       // allow 2 probe calls in HALF-OPEN before closing
    .build();
```

`CircuitBreakerPolicy.defaults()` gives reasonable out-of-the-box settings (50 % threshold, 5 minimum calls, window of 10, 60 s open window, 2 half-open probes).

**Key behaviours:**
- When OPEN, the step fast-fails with a `Failure` whose `cause()` is `CircuitBreakerOpenException` (carrying the step id and `retriableAfter()` timestamp) — no call is made to `execute()`.
- Circuit state is **per-`Pipeline` instance**, keyed by step id, and persists across `pipeline.execute(...)` calls. Different `Pipeline` instances maintain independent circuit state.
- When combined with `RetryPolicy`, the circuit evaluates the **final outcome of the retry loop**, not individual attempt outcomes — a step that retries successfully counts as one success, not N−1 failures.
- `minimumCalls` is a hard floor: the circuit will not open until at least that many failures have been observed, regardless of the failure-rate threshold. This prevents premature tripping during pipeline warm-up.
- Applies to sequential steps, foreach steps, and parallel branch steps.

A `step.circuit_open` log event is emitted at `WARN` level when the circuit fast-fails a call.

---

## Lifecycle hooks

Register an `onStart` / `onFinish` / `onError` listener at the pipeline boundary. Useful for distributed tracing spans, audit logs, and top-level error reporting.

```java
Pipeline<Order, OrderResult> pipeline = PipelineBuilder.start(Order.class)
    .then(validateStep)
    .then(chargeStep)
    .withLifecycle(new PipelineLifecycle<Order, OrderResult>() {
        @Override
        public void onStart(Order input, StepContext ctx) {
            String traceId = ctx.context().get(TRACE_ID_KEY);
            tracer.startSpan("order-pipeline", traceId);
        }
        @Override
        public void onFinish(Result<OrderResult> result, StepContext ctx) {
            tracer.finishSpan();
        }
        @Override
        public void onError(Failure<OrderResult> failure, StepContext ctx) {
            alerts.send("order-pipeline", failure.failedStepId(), failure.cause());
        }
    })
    .build();
```

All three methods have default no-op implementations on the interface, so you only override what you need. Exceptions thrown by hook callbacks are caught and logged as `lifecycle.hook_failed` warnings — they never affect the pipeline result.

If `onStart` throws, execution halts immediately (no steps run) and the pipeline returns a `Failure` with `failedStepId="pipeline.onStart"`. `onFinish` and `onError` are still called with that failure so clean-up and alerting hooks always fire.

Hooks fire at the **top-level pipeline boundary only**. Sub-pipelines used inside `.branch()` arms do not re-fire the parent pipeline's hooks.

---

## Observability

### SLF4J structured logging

Every step invocation automatically emits three structured log events — no step code required:

| Event | Level | When |
|---|---|---|
| `step.start` | INFO | Before the first (or only) attempt |
| `step.finish` | INFO | After a successful attempt |
| `step.error` | ERROR | After a failed final attempt |
| `step.retry` | WARN | Before each retry attempt |
| `step.circuit_open` | WARN | When a circuit breaker fast-fails a step |
| `step.skip` | DEBUG | When a branch arm step is skipped |

Every event carries: `step.id`, `step.attempt`, `step.duration_ms`, `step.outcome`. Error events add `step.error_class` and `step.error_message`. Every `RequestContext` entry is added as additional structured key-value fields, making distributed log correlation automatic.

Configure your SLF4J backend (Logback, Log4j2, etc.) the usual way; no FlowPipe-specific configuration is required.

### MetricsRecorder SPI

Implement `MetricsRecorder` to receive per-step metrics calls after every execution:

```java
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.StepOutcome;

class MicrometerRecorder implements MetricsRecorder {
    private final MeterRegistry registry;

    @Override
    public void recordStepDuration(String stepId, long durationNanos) {
        registry.timer("flowpipe.step.duration", "step", stepId)
            .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void recordStepAttempts(String stepId, int attempts) {
        registry.counter("flowpipe.step.attempts", "step", stepId)
            .increment(attempts);
    }

    @Override
    public void recordStepOutcome(String stepId, StepOutcome outcome) {
        registry.counter("flowpipe.step.outcome", "step", stepId,
                         "outcome", outcome.name())
            .increment();
    }

    @Override
    public void recordRetryAttempt(String stepId, int attempt) {
        registry.counter("flowpipe.step.retries", "step", stepId)
            .increment();
    }
}

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .then(parse).then(describe)
    .withMetrics(new MicrometerRecorder(registry))
    .build();
```

`StepOutcome` has three values: `SUCCESS`, `FAILURE`, `SKIPPED`. Recorder exceptions are caught and logged as `metrics.recorder_failed` warnings — they never affect the pipeline result.

For a per-call override (useful in tests), use the three-argument `execute(input, context, recorder)` overload.

### SpanRecorder SPI

Implement `SpanRecorder` for automatic distributed tracing around every step — no lifecycle code needed.

```java
import io.flowpipe.observability.SpanRecorder;
import io.flowpipe.observability.StepOutcome;

class OtelSpanRecorder implements SpanRecorder {
    private final Tracer tracer;

    @Override
    public Object startStep(String stepId, RequestContext context) {
        String traceId = context.get(TRACE_ID_KEY);
        return tracer.spanBuilder(stepId)
            .setAttribute("trace.id", traceId)
            .startSpan();
    }

    @Override
    public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
        Span otelSpan = (Span) span;
        if (cause != null) otelSpan.recordException(cause);
        otelSpan.setAttribute("outcome", outcome.name());
        otelSpan.end();
    }
}

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .then(fetchStep)
    .then(enrichStep)
    .withTracing(new OtelSpanRecorder(tracer))
    .build();
```

`startStep` is called before the first attempt; `finishStep` is called after the final outcome — success, failure, or circuit-breaker trip. Skipped branch arm steps also fire both callbacks with `StepOutcome.SKIPPED`. Exceptions from either method are suppressed and logged as `tracing.recorder_failed` warnings.

---

## Input/output validation

Attach validators to any `StepDescriptor` to enforce pre- and post-conditions on step data.

```java
import io.flowpipe.validation.ValidationException;
import io.flowpipe.validation.Validator;

Validator<String> nonBlank = value -> {
    if (value == null || value.isBlank())
        throw new ValidationException("value must not be blank");
};

Validator<UserProfile> profileComplete = profile -> {
    if (profile.email() == null)
        throw new ValidationException("profile must have an email");
};

StepDescriptor<String, UserProfile> desc = StepDescriptor
    .builder("fetch-profile", String.class, UserProfile.class)
    .inputValidator(nonBlank)       // runs before execute()
    .outputValidator(profileComplete) // runs after execute(), before passing to next step
    .build();
```

`ValidationException` propagates as `Failure.cause()` with `failedStepId()` equal to the step's id, just like any other step exception.

---

## Pipeline introspection

`Pipeline.describe()` returns an immutable structural description of the pipeline — useful for diagnostics, audit logging, and tooling.

```java
Pipeline<String, OrderResult> pipeline = PipelineBuilder.start(String.class)
    .then(parseStep)
    .parallel2(OrderResult.class, combiner, enrichStep, validateStep)
    .build();

PipelineDescriptor desc = pipeline.describe();
desc.inputType();   // String.class
desc.outputType();  // OrderResult.class

for (NodeDescriptor node : desc.nodes()) {
    if (node instanceof NodeDescriptor.Step s) {
        StepDescriptor<?, ?> step = s.step();
        System.out.println("step: " + step.id()
            + " retry=" + step.retryPolicy().maxAttempts()
            + " timeout=" + step.timeoutPolicy().timeoutMs() + "ms");
    }
    if (node instanceof NodeDescriptor.Parallel p) {
        System.out.println("parallel block with " + p.branches().size() + " branches");
        p.branches().forEach(b -> System.out.println("  branch: " + b.id()));
    }
    if (node instanceof NodeDescriptor.Branch b) {
        System.out.println("branch: " + b.branchId());
        System.out.println("  ifTrue:  " + b.ifTrue().nodes().size() + " nodes");
        System.out.println("  ifFalse: " + b.ifFalse().nodes().size() + " nodes");
    }
    if (node instanceof NodeDescriptor.Foreach f) {
        System.out.println("foreach: " + f.step().id() + " concurrency=" + f.concurrency());
    }
}
```

`describe()` is pure — it reads no mutable state and is safe to call concurrently. It is **never read during execution** and adds no overhead to the request path.

---

## Test utilities

The `flowpipe-test` module provides utilities for unit-testing steps and pipelines.

### `StepHarness` — test a single step in isolation

```java
import io.flowpipe.test.StepHarness;

StateKey<String> tenantKey = StateKey.of("tenant", String.class);

StepHarness.Outcome<UserProfile> outcome = StepHarness.forStep(fetchProfileStep)
    .withContext(RequestContext.builder().put(TRACE_KEY, "t-123").build())
    .withState(tenantKey, "acme")
    .invoke(fetchProfileStep, "user-42");

assertThat(outcome.succeeded()).isTrue();
assertThat(outcome.value().email()).isEqualTo("user-42@acme.com");
assertThat(outcome.state().get(tenantKey)).isEqualTo("acme");
```

Use `outcome.error()` when testing failure paths.

### `RecordingMetricsRecorder` — capture metrics in tests

```java
import io.flowpipe.test.RecordingMetricsRecorder;

var recorder = new RecordingMetricsRecorder();

pipeline.execute(input, RequestContext.empty(), recorder);

List<RecordingMetricsRecorder.Event> events = recorder.events("fetch-profile");
assertThat(events).anyMatch(e -> e instanceof RecordingMetricsRecorder.OutcomeEvent oe
    && oe.outcome() == StepOutcome.SUCCESS);
```

### `RecordingPipelineLifecycle` — verify lifecycle hook invocations

```java
import io.flowpipe.test.RecordingPipelineLifecycle;

var lifecycle = new RecordingPipelineLifecycle<String, String>();

Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
    .then(myStep)
    .withLifecycle(lifecycle)
    .build();

pipeline.execute("hello");

assertThat(lifecycle.onStartInvocations()).hasSize(1);
assertThat(lifecycle.onFinishInvocations()).hasSize(1);
assertThat(lifecycle.onErrorInvocations()).isEmpty();
```

### `Steps` — factory for common test steps

```java
import io.flowpipe.test.Steps;

Step<String, String> identity  = Steps.identity("pass-through", String.class);
Step<String, String> throwing  = Steps.throwing("bad-step", String.class, new RuntimeException("boom"));
Step<String, Void>   noop      = Steps.noop("side-effect-only");
Step<String, List<String>> split = Steps.split("split", ",");
```

---

## Project layout

```
flowpipe-core/
  src/main/java/
    io.flowpipe.api          — public surface: Step (+ Step.builder(...) entry point),
                               StepBuilder, StepBuilder.Body, StepDescriptor, StepContext,
                               Result, Success, Failure, ExecutionTrace, TraceEntry,
                               RetryPolicy, TimeoutPolicy, CircuitBreakerPolicy,
                               StepTimeoutException, CircuitBreakerOpenException,
                               PipelineDeadlineExceededException, PipelineDescriptor,
                               NodeDescriptor, PipelineLifecycle, TriFunction, QuadFunction
    io.flowpipe.state        — State, StateKey, RequestContext, ContextKey
    io.flowpipe.validation   — Validator<T> SPI, ValidationException, NoOpValidator
    io.flowpipe.observability — MetricsRecorder SPI, SpanRecorder SPI, StepOutcome,
                               NoOpMetricsRecorder, NoOpSpanRecorder
    io.flowpipe.engine       — Pipeline (+ Pipeline.builder(...) alias), PipelineBuilder,
                               PipelineBuildException
                               (internals: FailsafePolicies, EngineNode subtypes)

flowpipe-test/
  src/main/java/
    io.flowpipe.test         — StepHarness, Steps, RecordingMetricsRecorder,
                               RecordingPipelineLifecycle

openspec/
  specs/                     — feature specifications
  changes/                   — proposals and designs for upcoming work
```

Runtime dependencies of `flowpipe-core`: `slf4j-api` and `dev.failsafe:failsafe:3.3.2` (resilience engine — retry, timeout, circuit breaker; kept as a private implementation detail, never exposed in the public API). No Spring, no external framework required.
