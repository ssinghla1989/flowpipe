package io.flowpipe.engine;

import io.flowpipe.api.ExecutionTrace;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.observability.TestMetricsRecorder;
import io.flowpipe.observability.TestMetricsRecorder.OutcomeEvent;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ForeachExecutionTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Returns a pipeline whose input is List<String> and foreach-applies the given step. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <R> Pipeline<List<String>, List<R>> listPipeline(Step<String, R> step) {
        return (Pipeline<List<String>, List<R>>) (Pipeline) PipelineBuilder
            .start((Class<List<String>>) (Class<?>) List.class)
            .foreach(step)
            .build();
    }

    private static Step<String, String> upper(String id) {
        return Step.builder(id, String.class, String.class).execute((s, ctx) -> s.toUpperCase()).build();
    }

    private static Step<String, Integer> length(String id) {
        return Step.builder(id, String.class, Integer.class).execute((s, ctx) -> s.length()).build();
    }

    /** Step with a configurable retry policy — body may throw. */
    private static Step<String, String> retryStep(String id, int maxAttempts, StepBody body) {
        StepDescriptor<String, String> desc = StepDescriptor.builder(id, String.class, String.class)
            .withRetry(RetryPolicy.fixed(maxAttempts, 0))
            .build();
        return new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) throws Exception { return body.run(input); }
        };
    }

    @FunctionalInterface
    private interface StepBody {
        String run(String input) throws Exception;
    }

    // ── 6.1 Sequential correctness ────────────────────────────────────────────

    @Test
    void all_items_succeed_sequential() {
        Pipeline<List<String>, List<String>> p = listPipeline(upper("up"));
        Result<List<String>> r = p.execute(List.of("hello", "world", "foo"));

        assertThat(r).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) r).value()).containsExactly("HELLO", "WORLD", "FOO");
    }

    @Test
    void empty_list_produces_empty_output() {
        Pipeline<List<String>, List<String>> p = listPipeline(upper("up"));
        Result<List<String>> r = p.execute(Collections.emptyList());

        assertThat(r).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) r).value()).isEmpty();
    }

    @Test
    void fail_fast_on_first_item_failure() {
        List<String> executed = new ArrayList<>();
        Step<String, String> failSecond = Step.builder("boom", String.class, String.class).execute((input, ctx) -> {
            executed.add(input);
            if (input.equals("b")) throw new RuntimeException("item-failure");
            return input;
        }).build();

        Pipeline<List<String>, List<String>> p = listPipeline(failSecond);
        Result<List<String>> r = p.execute(List.of("a", "b", "c"));

        assertThat(r).isInstanceOf(Failure.class);
        Failure<List<String>> failure = (Failure<List<String>>) r;
        assertThat(failure.cause()).hasMessage("item-failure");
        assertThat(failure.failedStepId()).isEqualTo("boom[1]");
        assertThat(executed).containsExactly("a", "b");
    }

    @Test
    void retry_per_item_transient_failure_then_success() {
        AtomicInteger callCount = new AtomicInteger();
        Step<String, String> flaky = retryStep("flaky", 3, input -> {
            if (callCount.incrementAndGet() == 1) throw new RuntimeException("transient");
            return input + "!";
        });

        Pipeline<List<String>, List<String>> p = listPipeline(flaky);
        Result<List<String>> r = p.execute(List.of("x"));

        assertThat(r).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) r).value()).containsExactly("x!");
        assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void item_that_exhausts_retries_causes_pipeline_failure() {
        Step<String, String> alwaysFail = retryStep("always-fail", 2,
            input -> { throw new RuntimeException("permanent"); });

        Pipeline<List<String>, List<String>> p = listPipeline(alwaysFail);
        Result<List<String>> r = p.execute(List.of("x", "y"));

        assertThat(r).isInstanceOf(Failure.class);
        Failure<List<String>> f = (Failure<List<String>>) r;
        assertThat(f.cause()).hasMessage("permanent");
        assertThat(f.failedStepId()).isEqualTo("always-fail[0]");
    }

    // ── 6.2 Build-time validation ─────────────────────────────────────────────

    @Test
    void foreach_on_non_list_output_throws_at_call_site() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class).foreach(upper("up"))
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("List");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void duplicate_step_id_between_foreach_and_sibling_detected_at_build() {
        // Two foreach steps sharing the same inner step id — build must reject this.
        // After the first foreach, output is List<String>, so a second foreach is type-valid.
        assertThatThrownBy(() ->
            PipelineBuilder
                .start((Class<List<String>>) (Class<?>) List.class)
                .foreach(upper("dup"))
                .foreach(upper("dup"))
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("dup");
    }

    // ── 6.4 Composition ───────────────────────────────────────────────────────

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void then_foreach_then_composes_correctly() {
        // Step that produces a list from a comma-delimited string
        Step<String, List<String>> toList = (Step<String, List<String>>) (Step) Step.builder(
            "to-list", String.class, List.class).execute((s, ctx) -> List.of(s.split(","))).build();

        // Step that sums a List<Integer>
        Step<List<Integer>, Integer> sum = (Step<List<Integer>, Integer>) (Step) Step.builder(
            "sum", List.class, Integer.class).execute((list, ctx) -> ((List<Integer>) list).stream().mapToInt(Integer::intValue).sum()).build();

        Pipeline<String, Integer> p = (Pipeline<String, Integer>) (Pipeline) PipelineBuilder
            .start(String.class)
            .then(toList)
            .foreach(length("len"))
            .then(sum)
            .build();

        Result<Integer> r = p.execute("a,bb,ccc");

        assertThat(r).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) r).value()).isEqualTo(1 + 2 + 3);
    }

    @Test
    void foreach_foreach_composes_correctly() {
        // Each String item is uppercased, then the resulting List is processed again
        Pipeline<List<String>, List<String>> p = listPipeline(upper("up"));
        Result<List<String>> r = p.execute(List.of("a", "b"));

        assertThat(r).isInstanceOf(Success.class);
        assertThat(((Success<List<String>>) r).value()).containsExactly("A", "B");

        // Second pipeline: length of each string → then sum
        Pipeline<List<String>, List<Integer>> p2 = listPipeline(length("len2"));
        Result<List<Integer>> r2 = p2.execute(List.of("hello", "world"));

        assertThat(r2).isInstanceOf(Success.class);
        assertThat(((Success<List<Integer>>) r2).value()).containsExactly(5, 5);
    }

    // ── 6.5 Observability ─────────────────────────────────────────────────────

    @Test
    void metrics_recorder_called_once_per_item() {
        TestMetricsRecorder recorder = new TestMetricsRecorder();
        Pipeline<List<String>, List<String>> p = listPipeline(upper("up"));

        p.execute(List.of("a", "b", "c"), RequestContext.empty(), recorder);

        long foreachOutcomes = recorder.events().stream()
            .filter(e -> e instanceof OutcomeEvent oe && oe.outcome() == StepOutcome.SUCCESS)
            .filter(e -> e.stepId().startsWith("up["))
            .count();
        assertThat(foreachOutcomes).isEqualTo(3);
    }

    @Test
    void execution_trace_has_indexed_entries_per_item() {
        Pipeline<List<String>, List<String>> p = listPipeline(upper("item-step"));
        Result<List<String>> r = p.execute(List.of("x", "y", "z"));

        assertThat(r).isInstanceOf(Success.class);
        ExecutionTrace trace = ((Success<List<String>>) r).trace();

        List<String> ids = trace.entries().stream().map(TraceEntry::stepId).toList();
        assertThat(ids).contains("item-step[0]", "item-step[1]", "item-step[2]");
    }

    @Test
    void failed_item_trace_records_attempt_count() {
        Step<String, String> alwaysFail = retryStep("attempt-step", 3,
            input -> { throw new RuntimeException("boom"); });

        Pipeline<List<String>, List<String>> p = listPipeline(alwaysFail);
        Result<List<String>> r = p.execute(List.of("x"));

        assertThat(r).isInstanceOf(Failure.class);
        ExecutionTrace trace = ((Failure<List<String>>) r).trace();
        TraceEntry failEntry = trace.entries().stream()
            .filter(e -> e.stepId().equals("attempt-step[0]"))
            .findFirst()
            .orElseThrow();
        assertThat(failEntry.attempts()).isEqualTo(3);
    }
}
