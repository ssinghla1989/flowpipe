package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.state.ContextKey;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineAsStepTest {

    // -------------------------------------------------------------------------
    // Basic composition
    // -------------------------------------------------------------------------

    @Test
    void pipeline_as_step_in_then_runs_all_inner_steps() {
        Step<String, Integer> parse = Step.of("parse", String.class, Integer.class,
            (s, ctx) -> Integer.parseInt(s));
        Step<Integer, Integer> doubler = Step.of("double", Integer.class, Integer.class,
            (n, ctx) -> n * 2);

        Pipeline<String, Integer> inner = PipelineBuilder.start(String.class)
            .then(parse)
            .then(doubler)
            .build();

        Step<Integer, Integer> addTen = Step.of("addTen", Integer.class, Integer.class,
            (n, ctx) -> n + 10);

        Pipeline<String, Integer> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("inner-pipeline"))
            .then(addTen)
            .build();

        Result<Integer> result = outer.execute("5");

        // inner: parse("5") = 5, double(5) = 10; outer: addTen(10) = 20
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(20);
    }

    @Test
    void pipeline_as_step_output_becomes_next_steps_input() {
        Step<String, String> exclaim = Step.of("exclaim", String.class, String.class,
            (s, ctx) -> s + "!");
        Step<String, String> upper = Step.of("upper", String.class, String.class,
            (s, ctx) -> s.toUpperCase());

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(exclaim)
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("shout"))
            .then(upper)
            .build();

        Result<String> result = outer.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("HELLO!");
    }

    // -------------------------------------------------------------------------
    // Scatter-gather via foreach
    // -------------------------------------------------------------------------

    @Test
    void pipeline_as_step_in_foreach_applies_multi_step_transform_per_element() {
        // Each element goes through a two-step inner pipeline: trim → uppercase
        Step<String, String> trim = Step.of("trim", String.class, String.class,
            (s, ctx) -> s.trim());
        Step<String, String> upper = Step.of("upper", String.class, String.class,
            (s, ctx) -> s.toUpperCase());

        Pipeline<String, String> enrichPipeline = PipelineBuilder.start(String.class)
            .then(trim)
            .then(upper)
            .build();

        @SuppressWarnings({"unchecked", "rawtypes"})
        Step<String, List<String>> toList = (Step) Step.of("toList", String.class, List.class,
            (s, ctx) -> List.of(" hello ", " world "));

        @SuppressWarnings({"unchecked", "rawtypes"})
        Pipeline<String, List<String>> outer = (Pipeline<String, List<String>>) (Pipeline)
            PipelineBuilder.start(String.class)
                .then(toList)
                .foreach(enrichPipeline.asStep("enrich"))
                .build();

        Result<List<String>> result = outer.execute("ignored");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) result).value()).containsExactly("HELLO", "WORLD");
    }

    @Test
    void pipeline_as_step_in_concurrent_foreach_applies_inner_pipeline_per_element() {
        Step<String, Integer> len = Step.of("len", String.class, Integer.class,
            (s, ctx) -> s.length());
        Step<Integer, String> fmt = Step.of("fmt", Integer.class, String.class,
            (n, ctx) -> "len=" + n);

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(len)
            .then(fmt)
            .build();

        @SuppressWarnings({"unchecked", "rawtypes"})
        Step<String, List<String>> toList = (Step) Step.of("toList", String.class, List.class,
            (s, ctx) -> List.of("ab", "cde", "f"));

        @SuppressWarnings({"unchecked", "rawtypes"})
        Pipeline<String, List<String>> outer = (Pipeline<String, List<String>>) (Pipeline)
            PipelineBuilder.start(String.class)
                .then(toList)
                .foreach(inner.asStep("measure"), 3)
                .build();

        Result<List<String>> result = outer.execute("x");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) result).value()).containsExactly("len=2", "len=3", "len=1");
    }

    // -------------------------------------------------------------------------
    // Parallel composition
    // -------------------------------------------------------------------------

    @Test
    void pipeline_as_step_works_as_parallel_branch() {
        Step<String, Integer> wordCount = Step.of("words", String.class, Integer.class,
            (s, ctx) -> s.split("\\s+").length);
        Step<Integer, String> fmtWords = Step.of("fmt-words", Integer.class, String.class,
            (n, ctx) -> "words=" + n);

        Pipeline<String, String> wordPipeline = PipelineBuilder.start(String.class)
            .then(wordCount)
            .then(fmtWords)
            .build();

        Step<String, Integer> charCount = Step.of("chars", String.class, Integer.class,
            (s, ctx) -> s.length());

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .parallel2(
                String.class,
                (wordResult, chars) -> wordResult + " chars=" + chars,
                wordPipeline.asStep("word-pipeline"),
                charCount)
            .build();

        Result<String> result = outer.execute("hello world");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("words=2 chars=11");
    }

    // -------------------------------------------------------------------------
    // Failure propagation
    // -------------------------------------------------------------------------

    @Test
    void inner_failure_cause_propagates_to_outer_failure() {
        Step<String, String> bad = Step.of("bad", String.class, String.class,
            (s, ctx) -> { throw new IllegalStateException("inner error"); });

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(bad)
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("wrapped"))
            .build();

        Result<String> result = outer.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isInstanceOf(IllegalStateException.class);
        assertThat(failure.cause().getMessage()).isEqualTo("inner error");
    }

    @Test
    void outer_failure_reports_adapter_step_id_not_inner_step_id() {
        Step<String, String> bad = Step.of("inner-step", String.class, String.class,
            (s, ctx) -> { throw new RuntimeException("boom"); });

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(bad)
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("my-sub-pipeline"))
            .build();

        Result<String> result = outer.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        // The outer pipeline sees the adapter step's id, not the inner step's id
        assertThat(((Failure<String>) result).failedStepId()).isEqualTo("my-sub-pipeline");
    }

    @Test
    void steps_after_failed_adapter_step_do_not_run() {
        AtomicInteger afterCount = new AtomicInteger(0);

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(Step.of("bad", String.class, String.class,
                (s, ctx) -> { throw new RuntimeException("fail"); }))
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("sub"))
            .then(Step.of("after", String.class, String.class, (s, ctx) -> {
                afterCount.incrementAndGet();
                return s;
            }))
            .build();

        outer.execute("x");

        assertThat(afterCount.get()).isZero();
    }

    // -------------------------------------------------------------------------
    // Context propagation
    // -------------------------------------------------------------------------

    @Test
    void outer_request_context_is_forwarded_to_inner_pipeline() {
        ContextKey<String> tenantKey = ContextKey.of("tenant", String.class);
        AtomicReference<String> captured = new AtomicReference<>();

        Step<String, String> captureStep = Step.of("capture", String.class, String.class,
            (s, ctx) -> {
                captured.set(ctx.context().get(tenantKey));
                return s;
            });

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(captureStep)
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("inner"))
            .build();

        outer.execute("x", RequestContext.of(tenantKey, "tenant-123"));

        assertThat(captured.get()).isEqualTo("tenant-123");
    }

    // -------------------------------------------------------------------------
    // ExecutionTrace isolation
    // -------------------------------------------------------------------------

    @Test
    void outer_trace_contains_adapter_step_id_not_inner_step_ids() {
        Step<String, String> innerStep = Step.of("inner-s", String.class, String.class,
            (s, ctx) -> s.toUpperCase());

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(innerStep)
            .build();

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(inner.asStep("adapter"))
            .build();

        Result<String> result = outer.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        Success<String> success = (Success<String>) result;

        boolean hasAdapter = success.trace().entries().stream()
            .anyMatch(e -> e.stepId().equals("adapter"));
        boolean hasInnerStep = success.trace().entries().stream()
            .anyMatch(e -> e.stepId().equals("inner-s"));

        assertThat(hasAdapter).isTrue();
        assertThat(hasInnerStep).isFalse();
    }

    // -------------------------------------------------------------------------
    // Resilience policies on adapter step
    // -------------------------------------------------------------------------

    @Test
    void retry_policy_on_adapter_step_retries_entire_inner_pipeline() {
        AtomicInteger attempts = new AtomicInteger(0);

        Step<String, String> flaky = Step.of("flaky", String.class, String.class,
            (s, ctx) -> {
                int attempt = attempts.incrementAndGet();
                if (attempt < 3) throw new RuntimeException("transient");
                return "ok";
            });

        Pipeline<String, String> inner = PipelineBuilder.start(String.class)
            .then(flaky)
            .build();

        // Attach a retry policy by creating a custom Step wrapping the adapter
        Step<String, String> adapter = inner.asStep("retriable-sub");
        Step<String, String> retriableAdapter = new Step<>() {
            @Override
            public io.flowpipe.api.StepDescriptor<String, String> describe() {
                return io.flowpipe.api.StepDescriptor.builder("retriable-sub", String.class, String.class)
                    .withRetry(RetryPolicy.fixed(3, 0))
                    .build();
            }

            @Override
            public String execute(String input, io.flowpipe.api.StepContext ctx) throws Exception {
                return adapter.execute(input, ctx);
            }
        };

        Pipeline<String, String> outer = PipelineBuilder.start(String.class)
            .then(retriableAdapter)
            .build();

        Result<String> result = outer.execute("x");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }
}
