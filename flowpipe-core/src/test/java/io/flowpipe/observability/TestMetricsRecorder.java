package io.flowpipe.observability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-test mirror of flowpipe-test's RecordingMetricsRecorder. Lives here because
 * flowpipe-core's test classpath cannot depend on flowpipe-test (circular).
 */
public final class TestMetricsRecorder implements MetricsRecorder {

    public sealed interface Event permits DurationEvent, AttemptsEvent, OutcomeEvent,
        RetryAttemptEvent, PipelineDurationEvent, PipelineOutcomeEvent {
        String stepId();
    }

    public record DurationEvent(String stepId, long durationNanos) implements Event {}

    public record AttemptsEvent(String stepId, int attempts) implements Event {}

    public record OutcomeEvent(String stepId, StepOutcome outcome) implements Event {}

    public record RetryAttemptEvent(String stepId, int attemptNumber) implements Event {}

    public record PipelineDurationEvent(long durationNanos, StepOutcome outcome) implements Event {
        @Override public String stepId() { return "pipeline"; }
    }

    public record PipelineOutcomeEvent(StepOutcome outcome) implements Event {
        @Override public String stepId() { return "pipeline"; }
    }

    private final List<Event> events = new ArrayList<>();

    @Override
    public synchronized void recordStepDuration(String stepId, long durationNanos) {
        events.add(new DurationEvent(stepId, durationNanos));
    }

    @Override
    public synchronized void recordStepAttempts(String stepId, int attempts) {
        events.add(new AttemptsEvent(stepId, attempts));
    }

    @Override
    public synchronized void recordStepOutcome(String stepId, StepOutcome outcome) {
        events.add(new OutcomeEvent(stepId, outcome));
    }

    @Override
    public synchronized void recordRetryAttempt(String stepId, int attemptNumber) {
        events.add(new RetryAttemptEvent(stepId, attemptNumber));
    }

    @Override
    public synchronized void recordPipelineDuration(long durationNanos, StepOutcome outcome) {
        events.add(new PipelineDurationEvent(durationNanos, outcome));
    }

    @Override
    public synchronized void recordPipelineOutcome(StepOutcome outcome) {
        events.add(new PipelineOutcomeEvent(outcome));
    }

    public synchronized List<Event> events() {
        return Collections.unmodifiableList(new ArrayList<>(events));
    }

    public synchronized List<Event> events(String stepId) {
        List<Event> filtered = new ArrayList<>();
        for (Event e : events) {
            if (e.stepId().equals(stepId)) filtered.add(e);
        }
        return Collections.unmodifiableList(filtered);
    }

    public synchronized void clear() {
        events.clear();
    }
}
