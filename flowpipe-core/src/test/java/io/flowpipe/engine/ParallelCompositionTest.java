package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.observability.Slf4jTestAppender;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.observability.TestMetricsRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParallelCompositionTest {

    private Slf4jTestAppender appender;

    @AfterEach
    void detachAppender() {
        if (appender != null) {
            appender.detach();
            appender = null;
        }
    }

    // 5.1 — Both branches receive the same input
    @Test
    void both_branches_receive_the_same_input() {
        AtomicReference<String> seenA = new AtomicReference<>();
        AtomicReference<String> seenB = new AtomicReference<>();

        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> {
            seenA.set(s);
            return s;
        });
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> {
            seenB.set(s);
            return s;
        });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .parallel2(String.class, (a, b) -> a, stepA, stepB)
            .build();

        pipeline.execute("hello");

        assertThat(seenA.get()).isEqualTo("hello");
        assertThat(seenB.get()).isEqualTo("hello");
    }

    // 5.2 — Combiner receives branch outputs in declaration order
    @Test
    void combiner_receives_branch_outputs_in_declaration_order() {
        CountDownLatch bStarted = new CountDownLatch(1);

        // stepA waits until stepB has signalled, ensuring stepB completes first
        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> {
            try {
                bStarted.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return "A";
        });
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> {
            bStarted.countDown();
            return "B";
        });

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a + b, stepA, stepB)
                .withExecutor(exec)
                .build();

            Result<String> result = pipeline.execute("x");

            assertThat(result).isInstanceOf(Success.class);
            assertThat(((Success<String>) result).value()).isEqualTo("AB");
        } finally {
            exec.shutdown();
        }
    }

    // 5.3 — One failing branch produces Failure with that branch's step id; post-parallel step not invoked
    @Test
    void one_failing_branch_produces_failure_and_skips_subsequent_steps() {
        AtomicReference<Boolean> afterRan = new AtomicReference<>(false);

        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class,
            (s, ctx) -> { throw new RuntimeException("boom"); });
        Step<String, String> after = Step.of("after", String.class, String.class, (s, ctx) -> {
            afterRan.set(true);
            return s;
        });

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stepA, stepB)
                .withExecutor(exec)
                .then(after)
                .build();

            Result<String> result = pipeline.execute("x");

            assertThat(result).isInstanceOf(Failure.class);
            assertThat(((Failure<String>) result).failedStepId()).isEqualTo("b");
            assertThat(afterRan.get()).isFalse();
        } finally {
            exec.shutdown();
        }
    }

    // 5.4 — Failure.cause() is the branch's original exception (not wrapped in ExecutionException)
    @Test
    void failure_cause_is_original_branch_exception() {
        IllegalStateException root = new IllegalStateException("root cause");
        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class,
            (s, ctx) -> { throw root; });

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stepA, stepB)
                .withExecutor(exec)
                .build();

            Result<String> result = pipeline.execute("x");

            assertThat(result).isInstanceOf(Failure.class);
            assertThat(((Failure<String>) result).cause()).isSameAs(root);
        } finally {
            exec.shutdown();
        }
    }

    // 5.5 — parallel2 advances builder output type to combiner return type (compile-time check)
    @Test
    void parallel2_advances_output_type_to_combiner_return_type() {
        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, Integer> stepB = Step.of("b", String.class, Integer.class, (s, ctx) -> s.length());

        // Combiner: BiFunction<String, Integer, String> — result type is String, no cast needed
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .parallel2(String.class, (a, b) -> a + b, stepA, stepB)
            .build();

        Result<String> result = pipeline.execute("hi");
        assertThat(result).isInstanceOf(Success.class);
        // If this compiles without casting, 5.5 is satisfied
        Success<String> success = (Success<String>) result;
        assertThat(success.value()).isEqualTo("hi2");
    }

    // 5.6 — Per-branch step.start and step.finish events emitted (one per branch id)
    @Test
    void per_branch_start_and_finish_events_are_emitted() {
        appender = Slf4jTestAppender.attachToEngine();

        Step<String, String> stepA = Step.of("stepA", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("stepB", String.class, String.class, (s, ctx) -> s);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stepA, stepB)
                .withExecutor(exec)
                .build()
                .execute("x");
        } finally {
            exec.shutdown();
        }

        long startA = appender.events("step.start").stream()
            .filter(e -> "stepA".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();
        long startB = appender.events("step.start").stream()
            .filter(e -> "stepB".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();
        long finishA = appender.events("step.finish").stream()
            .filter(e -> "stepA".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();
        long finishB = appender.events("step.finish").stream()
            .filter(e -> "stepB".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();

        assertThat(startA).isEqualTo(1);
        assertThat(startB).isEqualTo(1);
        assertThat(finishA).isEqualTo(1);
        assertThat(finishB).isEqualTo(1);
    }

    // 5.7 — Failing branch produces step.error (not step.finish) for that branch's id
    @Test
    void failing_branch_produces_step_error_not_step_finish() {
        appender = Slf4jTestAppender.attachToEngine();

        Step<String, String> stable = Step.of("stable", String.class, String.class, (s, ctx) -> s);
        Step<String, String> unstable = Step.of("unstable", String.class, String.class,
            (s, ctx) -> { throw new RuntimeException("boom"); });

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stable, unstable)
                .withExecutor(exec)
                .build()
                .execute("x");
        } finally {
            exec.shutdown();
        }

        long errorCount = appender.events("step.error").stream()
            .filter(e -> "unstable".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();
        long finishCount = appender.events("step.finish").stream()
            .filter(e -> "unstable".equals(Slf4jTestAppender.fields(e).get("step.id")))
            .count();

        assertThat(errorCount).isEqualTo(1);
        assertThat(finishCount).isEqualTo(0);
    }

    // 5.8 — Per-branch recorder receives duration + attempts + outcome for each branch
    @Test
    void per_branch_recorder_receives_duration_attempts_outcome() {
        TestMetricsRecorder rec = new TestMetricsRecorder();

        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> s);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stepA, stepB)
                .withExecutor(exec)
                .withMetrics(rec)
                .build()
                .execute("x");
        } finally {
            exec.shutdown();
        }

        for (String id : List.of("a", "b")) {
            List<TestMetricsRecorder.Event> events = rec.events(id);
            assertThat(events).filteredOn(e -> e instanceof TestMetricsRecorder.DurationEvent).hasSize(1);
            assertThat(events).filteredOn(e -> e instanceof TestMetricsRecorder.AttemptsEvent).hasSize(1);
            assertThat(events).filteredOn(e -> e instanceof TestMetricsRecorder.OutcomeEvent)
                .extracting(e -> ((TestMetricsRecorder.OutcomeEvent) e).outcome())
                .containsExactly(StepOutcome.SUCCESS);
        }
    }

    // 5.9 — Success trace contains entries for all branches
    @Test
    void success_trace_contains_entries_for_all_branches() {
        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .parallel2(String.class, (a, b) -> a, stepA, stepB)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Success.class);
        List<TraceEntry> entries = ((Success<String>) result).trace().entries();
        assertThat(entries).extracting(TraceEntry::stepId)
            .containsExactlyInAnyOrder("a", "b");
    }

    // 5.10 — Shut-down executor causes Failure, not unchecked exception
    @Test
    void shutdown_executor_causes_failure_not_exception() {
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.shutdown();

        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .parallel2(String.class, (a, b) -> a, stepA, stepB)
            .withExecutor(exec)
            .build();

        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
    }

    // 5.11 — build() rejects a parallelN block with fewer than 2 branches
    @Test
    void build_rejects_parallelN_with_fewer_than_two_branches() {
        Step<String, String> only = Step.of("only", String.class, String.class, (s, ctx) -> s);
        Map<String, Step<String, ?>> singleEntry = new LinkedHashMap<>();
        singleEntry.put("only", only);

        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .parallelN(String.class, singleEntry, m -> (String) m.get("only"))
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("2");
    }

    // 5.12 — build() rejects a step id duplicated across a sequential step and a parallel branch
    @Test
    void build_rejects_duplicate_id_across_sequential_and_parallel() {
        Step<String, String> seq = Step.of("validate", String.class, String.class, (s, ctx) -> s);
        Step<String, String> branchA = Step.of("validate", String.class, String.class, (s, ctx) -> s);
        Step<String, String> branchB = Step.of("other", String.class, String.class, (s, ctx) -> s);

        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .then(seq)
                .parallel2(String.class, (a, b) -> a, branchA, branchB)
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("validate");
    }

    // 5.13 — build() rejects a parallelN map key that disagrees with the step's descriptor id
    @Test
    void build_rejects_parallelN_key_that_disagrees_with_step_id() {
        Step<String, String> real = Step.of("real-name", String.class, String.class, (s, ctx) -> s);
        Step<String, String> other = Step.of("other", String.class, String.class, (s, ctx) -> s);
        Map<String, Step<String, ?>> steps = new LinkedHashMap<>();
        steps.put("alias", real);
        steps.put("other", other);

        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .parallelN(String.class, steps, m -> (String) m.get("alias"))
                .build()
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("alias")
            .hasMessageContaining("real-name");
    }

    // 5.14 — withExecutor(null) throws NullPointerException
    @Test
    void with_executor_null_throws_npe() {
        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class).withExecutor(null)
        ).isInstanceOf(NullPointerException.class);
    }

    // 5.16 — then() after parallel2 rejects a type mismatch at build time (regression: was silently bypassed)
    @Test
    void then_after_parallel2_rejects_downstream_type_mismatch() {
        Step<String, String> stepA = Step.of("a", String.class, String.class, (s, ctx) -> s);
        Step<String, String> stepB = Step.of("b", String.class, String.class, (s, ctx) -> s);
        Step<Integer, Integer> wrongNext = Step.of("wrong", Integer.class, Integer.class, (n, ctx) -> n);

        assertThatThrownBy(() ->
            PipelineBuilder.start(String.class)
                .parallel2(String.class, (a, b) -> a, stepA, stepB)
                .then(wrongNext)
        ).isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("wrong")
            .hasMessageContaining("Integer")
            .hasMessageContaining("String");
    }

    // 5.15 — parallelN combiner receives a Map keyed by step ids with correct output values
    @Test
    void parallelN_combiner_receives_map_keyed_by_step_ids() {
        Step<String, Integer> x = Step.of("x", String.class, Integer.class, (s, ctx) -> 1);
        Step<String, Integer> y = Step.of("y", String.class, Integer.class, (s, ctx) -> 2);

        Map<String, Step<String, ?>> steps = new LinkedHashMap<>();
        steps.put("x", x);
        steps.put("y", y);

        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .parallelN(Integer.class, steps, m -> (Integer) m.get("x") + (Integer) m.get("y"))
            .build();

        Result<Integer> result = pipeline.execute("ignored");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<Integer>) result).value()).isEqualTo(3);
    }
}
