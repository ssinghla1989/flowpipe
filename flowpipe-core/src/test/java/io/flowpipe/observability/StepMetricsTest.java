package io.flowpipe.observability;

import io.flowpipe.api.Step;
import io.flowpipe.api.Success;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;
import io.flowpipe.observability.TestMetricsRecorder.AttemptsEvent;
import io.flowpipe.observability.TestMetricsRecorder.DurationEvent;
import io.flowpipe.observability.TestMetricsRecorder.Event;
import io.flowpipe.observability.TestMetricsRecorder.OutcomeEvent;
import io.flowpipe.observability.TestMetricsRecorder.PipelineDurationEvent;
import io.flowpipe.observability.TestMetricsRecorder.PipelineOutcomeEvent;
import io.flowpipe.state.RequestContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StepMetricsTest {

    @Test
    void noop_recorder_methods_are_silent() {
        NoOpMetricsRecorder.instance().recordStepDuration("x", 1L);
        NoOpMetricsRecorder.instance().recordStepAttempts("x", 1);
        NoOpMetricsRecorder.instance().recordStepOutcome("x", StepOutcome.SUCCESS);
        // assertion: no exception
    }

    // Each execute emits 5 events: 3 per-step (duration, attempts, outcome) + 2 pipeline-level
    // (pipeline duration, pipeline outcome). See pipeline_metrics_emit_* tests below.
    private static final int EVENTS_PER_EXECUTE = 5;

    @Test
    void builder_default_is_no_op_when_with_metrics_not_called() {
        Step<String, String> s = Step.builder("s", String.class, String.class).execute((in, ctx) -> in).build();
        Pipeline<String, String> defaulted = PipelineBuilder.start(String.class).then(s).build();

        TestMetricsRecorder override = new TestMetricsRecorder();
        defaulted.execute("x", RequestContext.empty(), override);
        assertThat(override.events()).hasSize(EVENTS_PER_EXECUTE);

        override.clear();
        defaulted.execute("x");
        assertThat(override.events()).isEmpty();
    }

    @Test
    void with_metrics_replaces_previously_configured_recorder() {
        TestMetricsRecorder a = new TestMetricsRecorder();
        TestMetricsRecorder b = new TestMetricsRecorder();
        Step<String, String> s = Step.builder("s", String.class, String.class).execute((in, ctx) -> in).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(s)
            .withMetrics(a)
            .withMetrics(b)
            .build();
        pipeline.execute("x");

        assertThat(a.events()).isEmpty();
        assertThat(b.events()).hasSize(EVENTS_PER_EXECUTE);
    }

    @Test
    void per_call_recorder_override_receives_only_that_call_and_default_receives_subsequent() {
        TestMetricsRecorder defaultRec = new TestMetricsRecorder();
        TestMetricsRecorder override = new TestMetricsRecorder();
        Step<String, String> s = Step.builder("s", String.class, String.class).execute((in, ctx) -> in).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(s)
            .withMetrics(defaultRec)
            .build();

        pipeline.execute("x", RequestContext.empty(), override);
        assertThat(override.events()).hasSize(EVENTS_PER_EXECUTE);
        assertThat(defaultRec.events()).isEmpty();

        pipeline.execute("y");
        assertThat(override.events()).hasSize(EVENTS_PER_EXECUTE);
        assertThat(defaultRec.events()).hasSize(EVENTS_PER_EXECUTE);
    }

    @Test
    void success_path_records_one_duration_attempts_outcome_per_step() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> s1 = Step.builder("s1", Integer.class, Integer.class).execute((i, ctx) -> i).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(s1)
            .withMetrics(rec)
            .build();
        pipeline.execute(1);

        List<Event> events = rec.events("s1");
        assertThat(events).hasSize(3);
        assertThat(events).filteredOn(e -> e instanceof AttemptsEvent)
            .extracting(e -> ((AttemptsEvent) e).attempts())
            .containsExactly(1);
        assertThat(events).filteredOn(e -> e instanceof OutcomeEvent)
            .extracting(e -> ((OutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.SUCCESS);
        assertThat(events).filteredOn(e -> e instanceof DurationEvent).hasSize(1);
    }

    @Test
    void failure_path_records_failure_for_failing_step_and_success_for_prior_step() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> ok = Step.builder("ok", Integer.class, Integer.class).execute((i, ctx) -> i).build();
        Step<Integer, Integer> bad = Step.builder("bad", Integer.class, Integer.class).execute((i, ctx) -> { throw new RuntimeException("boom"); }).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(ok).then(bad)
            .withMetrics(rec)
            .build();
        pipeline.execute(1);

        assertThat(rec.events("ok")).filteredOn(e -> e instanceof OutcomeEvent)
            .extracting(e -> ((OutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.SUCCESS);
        assertThat(rec.events("bad")).filteredOn(e -> e instanceof OutcomeEvent)
            .extracting(e -> ((OutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.FAILURE);
        assertThat(rec.events("bad")).filteredOn(e -> e instanceof AttemptsEvent)
            .extracting(e -> ((AttemptsEvent) e).attempts())
            .containsExactly(1);
    }

    @Test
    void pipeline_metrics_emit_one_duration_and_one_outcome_per_execute_on_success() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<String, String> s = Step.builder("s", String.class, String.class).execute((in, ctx) -> in).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(s)
            .withMetrics(rec)
            .build();
        pipeline.execute("x");

        assertThat(rec.events()).filteredOn(e -> e instanceof PipelineDurationEvent)
            .extracting(e -> ((PipelineDurationEvent) e).outcome())
            .containsExactly(StepOutcome.SUCCESS);
        assertThat(rec.events()).filteredOn(e -> e instanceof PipelineDurationEvent)
            .extracting(e -> ((PipelineDurationEvent) e).durationNanos())
            .allMatch(d -> d > 0L);
        assertThat(rec.events()).filteredOn(e -> e instanceof PipelineOutcomeEvent)
            .extracting(e -> ((PipelineOutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.SUCCESS);
    }

    @Test
    void pipeline_metrics_record_failure_outcome_when_a_step_fails() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<String, String> bad = Step.builder("bad", String.class, String.class)
            .execute((in, ctx) -> { throw new RuntimeException("boom"); }).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(bad)
            .withMetrics(rec)
            .build();
        pipeline.execute("x");

        assertThat(rec.events()).filteredOn(e -> e instanceof PipelineDurationEvent)
            .extracting(e -> ((PipelineDurationEvent) e).outcome())
            .containsExactly(StepOutcome.FAILURE);
        assertThat(rec.events()).filteredOn(e -> e instanceof PipelineOutcomeEvent)
            .extracting(e -> ((PipelineOutcomeEvent) e).outcome())
            .containsExactly(StepOutcome.FAILURE);
    }

    @Test
    void recorder_duration_equals_trace_entry_duration() {
        TestMetricsRecorder rec = new TestMetricsRecorder();
        Step<Integer, Integer> s = Step.builder("s", Integer.class, Integer.class).execute((i, ctx) -> i).build();

        Pipeline<Integer, Integer> pipeline = PipelineBuilder.start(Integer.class)
            .then(s)
            .withMetrics(rec)
            .build();
        Success<Integer> result = (Success<Integer>) pipeline.execute(1);

        long traceDuration = result.trace().entries().stream()
            .filter(e -> e.stepId().equals("s"))
            .mapToLong(TraceEntry::durationNanos)
            .findFirst().orElseThrow();
        long recordedDuration = rec.events("s").stream()
            .filter(e -> e instanceof DurationEvent)
            .mapToLong(e -> ((DurationEvent) e).durationNanos())
            .findFirst().orElseThrow();

        assertThat(recordedDuration).isEqualTo(traceDuration);
    }
}
