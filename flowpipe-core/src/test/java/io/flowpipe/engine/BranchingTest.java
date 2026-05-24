package io.flowpipe.engine;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.observability.Slf4jTestAppender;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.observability.TestMetricsRecorder;
import io.flowpipe.observability.TestMetricsRecorder.OutcomeEvent;
import io.flowpipe.state.StateKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BranchingTest {

    // --- helpers ---

    private static Step<Integer, String> intToStr(String id) {
        return Step.of(id, Integer.class, String.class, (i, ctx) -> "v" + i);
    }

    private static Step<Integer, Integer> passInt(String id) {
        return Step.of(id, Integer.class, Integer.class, (i, ctx) -> i);
    }

    private static Pipeline<Integer, String> posArm() {
        return PipelineBuilder.start(Integer.class).then(intToStr("pos")).build();
    }

    private static Pipeline<Integer, String> negArm() {
        return PipelineBuilder.start(Integer.class).then(intToStr("neg")).build();
    }

    // --- logging ---

    private Slf4jTestAppender appender;

    @BeforeEach
    void attachLog() {
        appender = Slf4jTestAppender.attachToEngine();
    }

    @AfterEach
    void detachLog() {
        appender.detach();
    }

    // -------------------------------------------------------------------------
    // 5.1 branch() with matching types compiles and returns builder with correct output type
    // -------------------------------------------------------------------------

    @Test
    void branch_with_matching_types_compiles_and_output_type_is_correct() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        assertThat(pipeline.inputType()).isEqualTo(Integer.class);
        assertThat(pipeline.outputType()).isEqualTo(String.class);
    }

    // -------------------------------------------------------------------------
    // 5.2 build() rejects mismatched ifTrue input type
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void branch_rejects_mismatched_ifTrue_input_type() {
        Pipeline<String, String> wrongInputArm = PipelineBuilder.start(String.class)
            .then(Step.of("x", String.class, String.class, (s, ctx) -> s))
            .build();
        Pipeline<Integer, String> coerced = (Pipeline) wrongInputArm;
        Pipeline<Integer, String> correctArm = posArm();

        assertThatThrownBy(() ->
            PipelineBuilder.start(Integer.class)
                .branch("route", (val, ctx) -> true, coerced, correctArm))
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("route")
            .hasMessageContaining("ifTrue")
            .hasMessageContaining("String")
            .hasMessageContaining("Integer");
    }

    // -------------------------------------------------------------------------
    // 5.3 build() rejects mismatched ifFalse input type
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void branch_rejects_mismatched_ifFalse_input_type() {
        Pipeline<String, String> wrongInputArm = PipelineBuilder.start(String.class)
            .then(Step.of("x", String.class, String.class, (s, ctx) -> s))
            .build();
        Pipeline<Integer, String> coerced = (Pipeline) wrongInputArm;
        Pipeline<Integer, String> correctArm = posArm();

        assertThatThrownBy(() ->
            PipelineBuilder.start(Integer.class)
                .branch("route", (val, ctx) -> true, correctArm, coerced))
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("route")
            .hasMessageContaining("ifFalse")
            .hasMessageContaining("String")
            .hasMessageContaining("Integer");
    }

    // -------------------------------------------------------------------------
    // 5.4 build() rejects mismatched arm output types
    // -------------------------------------------------------------------------

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void branch_rejects_mismatched_arm_output_types() {
        Pipeline<Integer, String> strArm = PipelineBuilder.start(Integer.class)
            .then(Step.of("s", Integer.class, String.class, (i, ctx) -> "" + i))
            .build();
        Pipeline<Integer, Long> longArm = PipelineBuilder.start(Integer.class)
            .then(Step.of("l", Integer.class, Long.class, (i, ctx) -> (long) i))
            .build();

        assertThatThrownBy(() ->
            PipelineBuilder.start(Integer.class)
                .branch("split",
                    (val, ctx) -> true,
                    strArm,
                    (Pipeline<Integer, String>) (Pipeline) longArm))
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("split")
            .hasMessageContaining("String")
            .hasMessageContaining("Long");
    }

    // -------------------------------------------------------------------------
    // 5.5 build() rejects branch id duplicating a sequential step id
    // -------------------------------------------------------------------------

    @Test
    void build_rejects_branch_id_duplicating_sequential_step_id() {
        Step<Integer, Integer> enrich = passInt("enrich");

        assertThatThrownBy(() ->
            PipelineBuilder.start(Integer.class)
                .then(enrich)
                .branch("enrich", (val, ctx) -> true, posArm(), negArm())
                .build())
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("enrich");
    }

    // -------------------------------------------------------------------------
    // 5.6 build() rejects two branch nodes with the same id
    // -------------------------------------------------------------------------

    @Test
    void build_rejects_duplicate_branch_ids() {
        Pipeline<String, String> strArm = PipelineBuilder.start(String.class)
            .then(Step.of("x", String.class, String.class, (s, ctx) -> s))
            .build();

        Pipeline<Integer, String> firstBranch = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> true, posArm(), negArm())
            .build();

        assertThatThrownBy(() ->
            PipelineBuilder.start(Integer.class)
                .branch("route", (val, ctx) -> true, posArm(), negArm())
                .branch("route", (val, ctx) -> false,
                    PipelineBuilder.start(String.class).then(Step.of("a2", String.class, String.class, (s, ctx) -> s)).build(),
                    PipelineBuilder.start(String.class).then(Step.of("b2", String.class, String.class, (s, ctx) -> s)).build())
                .build())
            .isInstanceOf(PipelineBuildException.class)
            .hasMessageContaining("route");
    }

    // -------------------------------------------------------------------------
    // 5.7 predicate returning true routes to ifTrue arm
    // -------------------------------------------------------------------------

    @Test
    void predicate_true_routes_to_ifTrue_arm() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        Result<String> result = pipeline.execute(5);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("v5");
    }

    // -------------------------------------------------------------------------
    // 5.8 predicate returning false routes to ifFalse arm
    // -------------------------------------------------------------------------

    @Test
    void predicate_false_routes_to_ifFalse_arm() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("check", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        Result<String> result = pipeline.execute(-3);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("v-3");
    }

    // -------------------------------------------------------------------------
    // 5.9 predicate reads shared state to make routing decision
    // -------------------------------------------------------------------------

    @Test
    void predicate_can_read_shared_state() {
        StateKey<Boolean> FLAG = StateKey.of("flag", Boolean.class);

        Step<Integer, Integer> setter = Step.of("setter", Integer.class, Integer.class,
            (i, ctx) -> { ctx.state().set(FLAG, true); return i; });

        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .then(setter)
            .branch("stateCheck",
                (val, ctx) -> Boolean.TRUE.equals(ctx.state().get(FLAG)),
                posArm(),
                negArm())
            .build();

        Result<String> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("v1");
    }

    // -------------------------------------------------------------------------
    // 5.10 throwing predicate produces Failure with failedStepId == branchId
    // -------------------------------------------------------------------------

    @Test
    void throwing_predicate_produces_failure_with_branch_id() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route",
                (val, ctx) -> { throw new IllegalStateException("bad state"); },
                posArm(),
                negArm())
            .build();

        Result<String> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("route");
        assertThat(failure.cause()).isInstanceOf(IllegalStateException.class)
            .hasMessage("bad state");
    }

    // -------------------------------------------------------------------------
    // 5.11 trace order: branch node entry, then taken arm entries, then SKIPPED
    // -------------------------------------------------------------------------

    @Test
    void trace_contains_branch_entry_then_taken_then_skipped() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        Success<String> success = (Success<String>) pipeline.execute(5);

        List<TraceEntry> entries = success.trace().entries();
        assertThat(entries).extracting(TraceEntry::stepId)
            .containsExactly("route", "pos", "neg");
    }

    // -------------------------------------------------------------------------
    // 5.12 SKIPPED trace entries have durationNanos==0, attempts==0, skipped()==true
    // -------------------------------------------------------------------------

    @Test
    void skipped_trace_entries_have_correct_shape() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        Success<String> success = (Success<String>) pipeline.execute(5);

        TraceEntry skipped = success.trace().entries().stream()
            .filter(e -> e.stepId().equals("neg"))
            .findFirst()
            .orElseThrow();

        assertThat(skipped.skipped()).isTrue();
        assertThat(skipped.durationNanos()).isEqualTo(0L);
        assertThat(skipped.attempts()).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // 5.13 RecordingMetricsRecorder receives SKIPPED outcome for non-taken steps
    // -------------------------------------------------------------------------

    @Test
    void recorder_receives_skipped_outcome_for_non_taken_steps() {
        TestMetricsRecorder recorder = new TestMetricsRecorder();
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> val > 0, posArm(), negArm())
            .withMetrics(recorder)
            .build();

        pipeline.execute(5);

        List<TestMetricsRecorder.Event> negEvents = recorder.events("neg");
        assertThat(negEvents).hasSize(3);
        assertThat(negEvents).filteredOn(e -> e instanceof TestMetricsRecorder.DurationEvent)
            .extracting(e -> ((TestMetricsRecorder.DurationEvent) e).durationNanos())
            .containsExactly(0L);
        assertThat(negEvents).filteredOn(e -> e instanceof TestMetricsRecorder.AttemptsEvent)
            .extracting(e -> ((TestMetricsRecorder.AttemptsEvent) e).attempts())
            .containsExactly(0);
        assertThat(negEvents).filteredOn(e -> e instanceof OutcomeEvent)
            .extracting(e -> ((OutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.SKIPPED);
    }

    // -------------------------------------------------------------------------
    // 5.14 step.skip DEBUG log emitted for each non-taken step
    // -------------------------------------------------------------------------

    @Test
    void step_skip_debug_log_emitted_for_non_taken_step() {
        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> val > 0, posArm(), negArm())
            .build();

        pipeline.execute(5);

        List<ILoggingEvent> skipLogs = appender.events("step.skip");
        assertThat(skipLogs).hasSize(1);
        assertThat(skipLogs.get(0).getLevel()).isEqualTo(Level.DEBUG);
        Map<String, Object> fields = Slf4jTestAppender.fields(skipLogs.get(0));
        assertThat(fields)
            .containsEntry("step.id", "neg")
            .containsEntry("step.branch_id", "route");

        List<ILoggingEvent> startLogs = appender.events("step.start");
        assertThat(startLogs).noneMatch(e ->
            Slf4jTestAppender.fields(e).get("step.id").equals("neg"));
    }

    // -------------------------------------------------------------------------
    // 5.15 nested branch: outer ifTrue, inner ifFalse — trace depth-first
    // -------------------------------------------------------------------------

    @Test
    void nested_branch_trace_is_flattened_depth_first() {
        Pipeline<Integer, String> innerIfTrue = PipelineBuilder.start(Integer.class)
            .then(intToStr("inner-t"))
            .build();
        Pipeline<Integer, String> innerIfFalse = PipelineBuilder.start(Integer.class)
            .then(intToStr("inner-f"))
            .build();
        Pipeline<Integer, String> innerPipeline = PipelineBuilder.start(Integer.class)
            .branch("inner", (val, ctx) -> val < 0, innerIfTrue, innerIfFalse)
            .build();

        Pipeline<Integer, String> outerIfFalse = PipelineBuilder.start(Integer.class)
            .then(intToStr("outer-f"))
            .build();

        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("outer", (val, ctx) -> val > 0, innerPipeline, outerIfFalse)
            .build();

        // outer selects ifTrue (val=5 > 0), inner selects ifFalse (val=5 not < 0)
        Success<String> success = (Success<String>) pipeline.execute(5);

        assertThat(success.trace().entries()).extracting(TraceEntry::stepId)
            .containsExactly(
                "outer",        // branch node
                "inner",        // nested branch node (taken by outer)
                "inner-f",      // inner ifFalse (taken)
                "inner-t",      // inner ifTrue (skipped)
                "outer-f"       // outer ifFalse (skipped)
            );

        // inner-t is skipped (inner selected ifFalse), outer-f is skipped
        assertThat(success.trace().entries()).filteredOn(TraceEntry::skipped)
            .extracting(TraceEntry::stepId)
            .containsExactlyInAnyOrder("inner-t", "outer-f");
    }

    // -------------------------------------------------------------------------
    // 5.16 step failure inside taken arm produces Failure with that step's id
    // -------------------------------------------------------------------------

    @Test
    void step_failure_inside_taken_arm_uses_step_id_not_branch_id() {
        RuntimeException boom = new RuntimeException("step boom");
        Pipeline<Integer, String> failingArm = PipelineBuilder.start(Integer.class)
            .then(Step.of("explode", Integer.class, String.class,
                (i, ctx) -> { throw boom; }))
            .build();

        Pipeline<Integer, String> pipeline = PipelineBuilder.start(Integer.class)
            .branch("route", (val, ctx) -> true, failingArm, negArm())
            .build();

        Result<String> result = pipeline.execute(1);
        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("explode");
        assertThat(failure.cause()).isSameAs(boom);
    }
}
