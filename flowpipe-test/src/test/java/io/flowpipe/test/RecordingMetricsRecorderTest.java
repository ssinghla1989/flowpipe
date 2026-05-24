package io.flowpipe.test;

import io.flowpipe.observability.StepOutcome;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingMetricsRecorderTest {

    @Test
    void captures_calls_in_order() {
        RecordingMetricsRecorder rec = new RecordingMetricsRecorder();

        rec.recordStepDuration("a", 100L);
        rec.recordStepAttempts("a", 1);
        rec.recordStepOutcome("a", StepOutcome.SUCCESS);
        rec.recordStepDuration("b", 200L);
        rec.recordStepOutcome("b", StepOutcome.FAILURE);

        assertThat(rec.events()).containsExactly(
            new RecordingMetricsRecorder.DurationEvent("a", 100L),
            new RecordingMetricsRecorder.AttemptsEvent("a", 1),
            new RecordingMetricsRecorder.OutcomeEvent("a", StepOutcome.SUCCESS),
            new RecordingMetricsRecorder.DurationEvent("b", 200L),
            new RecordingMetricsRecorder.OutcomeEvent("b", StepOutcome.FAILURE)
        );
    }

    @Test
    void filters_events_by_step_id() {
        RecordingMetricsRecorder rec = new RecordingMetricsRecorder();
        rec.recordStepDuration("a", 1L);
        rec.recordStepDuration("b", 2L);

        assertThat(rec.events("a")).containsExactly(
            new RecordingMetricsRecorder.DurationEvent("a", 1L));
        assertThat(rec.events("b")).containsExactly(
            new RecordingMetricsRecorder.DurationEvent("b", 2L));
    }
}
