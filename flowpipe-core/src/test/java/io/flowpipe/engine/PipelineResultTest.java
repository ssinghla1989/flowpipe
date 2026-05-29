package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.api.TraceEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineResultTest {

    @Test
    void pattern_matching_instanceof_handles_both_arms_without_a_cast() {
        Result<Integer> r = new Success<>(1, io.flowpipe.api.ExecutionTrace.empty());
        String shape;
        if (r instanceof Success<Integer> s) {
            shape = "ok:" + s.value();
        } else if (r instanceof Failure<Integer> f) {
            shape = "err:" + f.failedStepId();
        } else {
            throw new AssertionError("Result is sealed: only Success or Failure are possible");
        }
        assertThat(shape).isEqualTo("ok:1");
    }

    @Test
    void success_carries_final_value_and_ordered_trace() {
        Step<Integer, Integer> a = Step.builder("a", Integer.class, Integer.class).execute((i, ctx) -> i + 1).build();
        Step<Integer, Integer> b = Step.builder("b", Integer.class, Integer.class).execute((i, ctx) -> i * 2).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(a).then(b).build();

        Result<Integer> result = pipeline.execute(10);

        assertThat(result).isInstanceOf(Success.class);
        Success<Integer> success = (Success<Integer>) result;
        assertThat(success.value()).isEqualTo(22);
        assertThat(success.trace().entries()).extracting(TraceEntry::stepId)
            .containsExactly("a", "b");
    }

    @Test
    void failure_trace_includes_executed_steps_up_to_and_including_failing_step() {
        RuntimeException boom = new RuntimeException("boom");
        Step<String, String> a = Step.builder("a", String.class, String.class).execute((s, ctx) -> s).build();
        Step<String, String> b = Step.builder("b", String.class, String.class).execute((s, ctx) -> { throw boom; }).build();
        Step<String, String> c = Step.builder("c", String.class, String.class).execute((s, ctx) -> s).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(a).then(b).then(c).build();
        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("b");
        assertThat(failure.trace().entries()).extracting(TraceEntry::stepId)
            .containsExactly("a", "b");
    }

    @Test
    void every_trace_entry_attempts_equals_one() {
        Step<Integer, Integer> a = Step.builder("a", Integer.class, Integer.class).execute((i, ctx) -> i).build();
        Step<Integer, Integer> b = Step.builder("b", Integer.class, Integer.class).execute((i, ctx) -> i).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(a).then(b).build();
        Success<Integer> success = (Success<Integer>) pipeline.execute(1);

        assertThat(success.trace().entries()).allSatisfy(entry -> {
            assertThat(entry.attempts()).isEqualTo(1);
            assertThat(entry.durationNanos()).isGreaterThanOrEqualTo(0L);
        });
    }
}
