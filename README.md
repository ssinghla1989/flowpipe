# FlowPipe

A Java library for orchestrating synchronous API pipelines. Compose typed, reusable steps into pipelines with automatic input/output validation, build-time wiring checks, and zero-boilerplate logging and metrics around every step.

Parallel composition has landed. Branching, retry, and lifecycle hooks are not yet implemented — they will land in subsequent changes.

## Requirements

- Java 17 or newer
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
    .parallel2((a, b) -> a + b, stepA, stepB)
    .withExecutor(Executors.newFixedThreadPool(4))
    .build();
```

**Lambda users**: on a 1-vCPU Lambda the common pool's parallelism is 0, which means branches execute on the calling thread and parallelism is illusory. Supply a `Executors.newCachedThreadPool()` executor (and shut it down after the invocation) to get real concurrency. FlowPipe never shuts down the executor it is given.

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
