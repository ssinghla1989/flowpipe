package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.api.TimeoutPolicy;
import io.flowpipe.state.StateKey;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for outputKey auto-state materialization (#1) and combiner-free parallel (#2).
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class OutputKeyTest {

    // ─── Feature 1: outputKey on StepDescriptor ──────────────────────────────

    // 1.1 — A step with outputKey writes its output to state after successful execution
    @Test
    void step_with_output_key_writes_output_to_state() {
        StateKey<String> KEY = StateKey.of("result", String.class);
        AtomicReference<String> observed = new AtomicReference<>();

        Step<String, String> producer = withKey("producer", String.class, String.class, KEY, s -> "produced-" + s);
        Step<String, String> consumer = Step.of("consumer", String.class, String.class,
            (s, ctx) -> { observed.set(ctx.state().get(KEY)); return s; });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(producer)
            .then(consumer)
            .build();

        assertThat(pipeline.execute("hello")).isInstanceOf(Success.class);
        assertThat(observed.get()).isEqualTo("produced-hello");
    }

    // 1.2 — A step without outputKey does not write to state
    @Test
    void step_without_output_key_does_not_write_state() {
        StateKey<String> KEY = StateKey.of("should-be-absent", String.class);
        AtomicReference<String> observed = new AtomicReference<>("sentinel");

        Step<String, String> checker = Step.of("checker", String.class, String.class,
            (s, ctx) -> { observed.set(ctx.state().get(KEY)); return s; });

        PipelineBuilder.start(String.class)
            .then(Step.of("pass", String.class, String.class, (s, ctx) -> s))
            .then(checker)
            .build()
            .execute("x");

        assertThat(observed.get()).isNull();
    }

    // 1.3 — outputKey writes state after a retry; the final successful output is stored
    @Test
    void output_key_writes_state_after_retry() {
        StateKey<String> KEY = StateKey.of("value", String.class);
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<String> observed = new AtomicReference<>();

        StepDescriptor<String, String> desc = StepDescriptor.builder("flaky", String.class, String.class)
            .withRetry(RetryPolicy.fixed(3, 0))
            .withOutputKey(KEY)
            .build();
        Step<String, String> flaky = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) {
                if (attempts.incrementAndGet() < 2) throw new RuntimeException("not yet");
                return "ok-" + input;
            }
        };

        Step<String, String> reader = Step.of("reader", String.class, String.class,
            (s, ctx) -> { observed.set(ctx.state().get(KEY)); return s; });

        PipelineBuilder.start(String.class).then(flaky).then(reader).build().execute("x");

        assertThat(observed.get()).isEqualTo("ok-x");
    }

    // 1.4 — If a step fails, its outputKey is NOT written to state
    @Test
    void failing_step_does_not_write_output_key_to_state() {
        StateKey<String> KEY = StateKey.of("value", String.class);
        AtomicReference<String> observed = new AtomicReference<>("sentinel");

        StepDescriptor<String, String> desc = StepDescriptor.builder("failing", String.class, String.class)
            .withOutputKey(KEY)
            .build();
        Step<String, String> failing = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) { throw new RuntimeException("boom"); }
        };
        Step<String, String> reader = Step.of("reader", String.class, String.class,
            (s, ctx) -> { observed.set(ctx.state().get(KEY)); return s; });

        Result<String> result = PipelineBuilder.start(String.class)
            .then(failing)
            .then(reader)
            .build()
            .execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(observed.get()).isEqualTo("sentinel"); // reader never ran
    }

    // 1.5 — withOutputKey can be stacked with withRetry, withTimeout on the same descriptor
    @Test
    void with_output_key_preserves_other_policies() {
        StateKey<String> KEY = StateKey.of("v", String.class);
        StepDescriptor<String, String> desc = StepDescriptor.builder("s", String.class, String.class)
            .withRetry(RetryPolicy.fixed(2, 0))
            .withOutputKey(KEY)
            .build();

        assertThat(desc.retryPolicy().maxAttempts()).isEqualTo(2);
        assertThat(desc.outputKey()).isEqualTo(KEY);
    }

    // 1.6 — withRetry/withTimeout/withCircuitBreaker on a descriptor that already has outputKey preserve it
    @Test
    void chained_with_methods_preserve_output_key() {
        StateKey<String> KEY = StateKey.of("v", String.class);
        StepDescriptor<String, String> base = StepDescriptor.builder("s", String.class, String.class)
            .withOutputKey(KEY)
            .build();

        assertThat(base.withRetry(RetryPolicy.fixed(3, 0)).outputKey()).isEqualTo(KEY);
        assertThat(base.withTimeout(TimeoutPolicy.ofMillis(500)).outputKey()).isEqualTo(KEY);
    }

    // 1.7 — outputKey on a foreach branch step writes each item's output to state (last item wins)
    @Test
    void foreach_step_with_output_key_writes_last_item_to_state() {
        StateKey<String> KEY = StateKey.of("last", String.class);

        Step<String, String> itemStep = withKey("item", String.class, String.class, KEY, s -> "done-" + s);
        AtomicReference<String> observed = new AtomicReference<>();

        Pipeline<List<String>, List<String>> pipeline =
            (Pipeline<List<String>, List<String>>) (Pipeline) PipelineBuilder
                .start((Class<List<String>>) (Class<?>) List.class)
                .foreach(itemStep)
                .then(Step.of("reader",
                    (Class<List<String>>) (Class<?>) List.class,
                    (Class<List<String>>) (Class<?>) List.class,
                    (list, ctx) -> { observed.set(ctx.state().get(KEY)); return list; }))
                .build();

        pipeline.execute(List.of("a", "b", "c"));

        assertThat(observed.get()).isEqualTo("done-c"); // sequential: last item writes last
    }

    // ─── Feature 2: combiner-free parallel ───────────────────────────────────

    // 2.1 — parallel2 without combiner writes both branch outputs to state via their outputKeys
    @Test
    void combiner_free_parallel2_writes_branch_outputs_to_state() {
        StateKey<String> KEY_A = StateKey.of("a-result", String.class);
        StateKey<Integer> KEY_B = StateKey.of("b-result", Integer.class);

        Step<String, String> stepA = withKey("step-a", String.class, String.class, KEY_A, s -> "A:" + s);
        Step<String, Integer> stepB = withKey("step-b", String.class, Integer.class, KEY_B, String::length);

        AtomicReference<String> seenA = new AtomicReference<>();
        AtomicReference<Integer> seenB = new AtomicReference<>();

        Step<String, String> reader = Step.of("reader", String.class, String.class, (s, ctx) -> {
            seenA.set(ctx.state().get(KEY_A));
            seenB.set(ctx.state().get(KEY_B));
            return s;
        });

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            Result<String> result = PipelineBuilder.start(String.class)
                .parallel2(stepA, stepB)
                .withExecutor(exec)
                .then(reader)
                .build()
                .execute("hello");

            assertThat(result).isInstanceOf(Success.class);
            assertThat(seenA.get()).isEqualTo("A:hello");
            assertThat(seenB.get()).isEqualTo(5);
        } finally {
            exec.shutdown();
        }
    }

    // 2.2 — combiner-free parallel2 passes the input through unchanged as the next pipeline value
    @Test
    void combiner_free_parallel2_passes_input_through_as_pipeline_value() {
        StateKey<String> KEY_A = StateKey.of("a", String.class);
        StateKey<String> KEY_B = StateKey.of("b", String.class);

        AtomicReference<String> pipelineValue = new AtomicReference<>();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            PipelineBuilder.start(String.class)
                .parallel2(
                    withKey("step-a", String.class, String.class, KEY_A, s -> "A"),
                    withKey("step-b", String.class, String.class, KEY_B, s -> "B"))
                .withExecutor(exec)
                .then(Step.of("next", String.class, String.class, (s, ctx) -> { pipelineValue.set(s); return s; }))
                .build()
                .execute("original");

            assertThat(pipelineValue.get()).isEqualTo("original");
        } finally {
            exec.shutdown();
        }
    }

    // 2.3 — combiner-free parallel3 writes all three outputs to state
    @Test
    void combiner_free_parallel3_writes_all_three_outputs_to_state() {
        StateKey<String> KEY_A = StateKey.of("a", String.class);
        StateKey<String> KEY_B = StateKey.of("b", String.class);
        StateKey<String> KEY_C = StateKey.of("c", String.class);

        AtomicReference<String> seenC = new AtomicReference<>();

        ExecutorService exec = Executors.newFixedThreadPool(3);
        try {
            PipelineBuilder.start(String.class)
                .parallel3(
                    withKey("a", String.class, String.class, KEY_A, s -> "A"),
                    withKey("b", String.class, String.class, KEY_B, s -> "B"),
                    withKey("c", String.class, String.class, KEY_C, s -> "C"))
                .withExecutor(exec)
                .then(Step.of("reader", String.class, String.class, (s, ctx) -> {
                    seenC.set(ctx.state().get(KEY_C));
                    return s;
                }))
                .build()
                .execute("x");

            assertThat(seenC.get()).isEqualTo("C");
        } finally {
            exec.shutdown();
        }
    }

    // 2.4 — combiner-free parallel4 writes all four outputs to state
    @Test
    void combiner_free_parallel4_writes_all_four_outputs_to_state() {
        StateKey<String> KEY_D = StateKey.of("d", String.class);
        AtomicReference<String> seenD = new AtomicReference<>();

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            PipelineBuilder.start(String.class)
                .parallel4(
                    withKey("a", String.class, String.class, StateKey.of("a", String.class), s -> "A"),
                    withKey("b", String.class, String.class, StateKey.of("b", String.class), s -> "B"),
                    withKey("c", String.class, String.class, StateKey.of("c", String.class), s -> "C"),
                    withKey("d", String.class, String.class, KEY_D, s -> "D"))
                .withExecutor(exec)
                .then(Step.of("reader", String.class, String.class,
                    (s, ctx) -> { seenD.set(ctx.state().get(KEY_D)); return s; }))
                .build()
                .execute("x");

            assertThat(seenD.get()).isEqualTo("D");
        } finally {
            exec.shutdown();
        }
    }

    // 2.5 — combiner-free parallelN(List) writes all outputs to state
    @Test
    void combiner_free_parallelN_writes_all_outputs_to_state() {
        StateKey<String> KEY_A = StateKey.of("a", String.class);
        StateKey<String> KEY_B = StateKey.of("b", String.class);

        AtomicReference<String> seenA = new AtomicReference<>();
        AtomicReference<String> seenB = new AtomicReference<>();

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            PipelineBuilder.start(String.class)
                .parallelN(List.of(
                    withKey("a", String.class, String.class, KEY_A, s -> "A"),
                    withKey("b", String.class, String.class, KEY_B, s -> "B")))
                .withExecutor(exec)
                .then(Step.of("reader", String.class, String.class, (s, ctx) -> {
                    seenA.set(ctx.state().get(KEY_A));
                    seenB.set(ctx.state().get(KEY_B));
                    return s;
                }))
                .build()
                .execute("x");

            assertThat(seenA.get()).isEqualTo("A");
            assertThat(seenB.get()).isEqualTo("B");
        } finally {
            exec.shutdown();
        }
    }

    // 2.6 — build() rejects combiner-free parallel2 when any branch is missing outputKey
    @Test
    void build_rejects_combiner_free_parallel2_with_missing_output_key() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .parallel2(
                    withKey("with-key", String.class, String.class, StateKey.of("k", String.class), s -> s),
                    Step.of("without-key", String.class, String.class, (s, ctx) -> s))
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("without-key")
            .hasMessageContaining("withOutputKey");
    }

    // 2.7 — build() rejects combiner-free parallelN(List) when any branch is missing outputKey
    @Test
    void build_rejects_combiner_free_parallelN_with_missing_output_key() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .parallelN(List.of(
                    withKey("a", String.class, String.class, StateKey.of("k", String.class), s -> s),
                    Step.of("b", String.class, String.class, (s, ctx) -> s)))
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("b")
            .hasMessageContaining("withOutputKey");
    }

    // 2.8 — combiner-free parallel branch failure propagates as Failure
    @Test
    void combiner_free_parallel2_branch_failure_propagates() {
        StepDescriptor<String, String> failingDesc = StepDescriptor.builder("step-b", String.class, String.class)
            .withOutputKey(StateKey.of("b", String.class))
            .build();
        Step<String, String> failingB = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return failingDesc; }
            @Override public String execute(String input, StepContext ctx) { throw new RuntimeException("branch-b-failed"); }
        };

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            Result<String> result = PipelineBuilder.start(String.class)
                .parallel2(
                    withKey("step-a", String.class, String.class, StateKey.of("a", String.class), s -> s),
                    failingB)
                .withExecutor(exec)
                .build()
                .execute("x");

            assertThat(result).isInstanceOf(Failure.class);
            assertThat(((Failure<String>) result).cause()).hasMessage("branch-b-failed");
        } finally {
            exec.shutdown();
        }
    }

    // 2.9 — combiner-free and combiner variants can be mixed in the same pipeline
    @Test
    void pipeline_can_mix_combiner_and_combiner_free_parallel_blocks() {
        StateKey<String> SIDE_KEY = StateKey.of("side", String.class);
        AtomicReference<String> sideValue = new AtomicReference<>();

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            Result<String> result = PipelineBuilder.start(String.class)
                // combiner-free block: both outputs go to state, input passes through
                .parallel2(
                    withKey("side", String.class, String.class, SIDE_KEY, s -> "side-" + s),
                    withKey("pass", String.class, String.class, StateKey.of("pass", String.class), s -> s))
                .withExecutor(exec)
                // combiner block: produces a combined result
                .parallel2(String.class, (l, r) -> l + "|" + r,
                    Step.of("left", String.class, String.class, (s, ctx) -> "L:" + s),
                    Step.of("right", String.class, String.class, (s, ctx) -> "R:" + s))
                .then(Step.of("reader", String.class, String.class, (s, ctx) -> {
                    sideValue.set(ctx.state().get(SIDE_KEY));
                    return s;
                }))
                .build()
                .execute("x");

            assertThat(result).isInstanceOf(Success.class);
            assertThat(((Success<String>) result).value()).isEqualTo("L:x|R:x");
            assertThat(sideValue.get()).isEqualTo("side-x");
        } finally {
            exec.shutdown();
        }
    }

    // 2.10 — parallelN(List) rejects a list containing null
    @Test
    void parallelN_list_rejects_null_element() {
        List<Step<String, ?>> stepsWithNull = java.util.Arrays.asList(null, null);
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class).parallelN(stepsWithNull)
        ).isInstanceOf(NullPointerException.class);
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    private static <I, O> Step<I, O> withKey(
            String id, Class<I> in, Class<O> out, StateKey<O> key, Function<I, O> fn) {
        StepDescriptor<I, O> desc = StepDescriptor.builder(id, in, out).withOutputKey(key).build();
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return desc; }
            @Override public O execute(I input, StepContext ctx) { return fn.apply(input); }
        };
    }
}
