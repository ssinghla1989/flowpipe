package io.flowpipe.observability;

public final class NoOpMetricsRecorder implements MetricsRecorder {

    private static final NoOpMetricsRecorder INSTANCE = new NoOpMetricsRecorder();

    private NoOpMetricsRecorder() {
    }

    public static NoOpMetricsRecorder instance() {
        return INSTANCE;
    }

    @Override
    public void recordStepDuration(String stepId, long durationNanos) {
        // no-op
    }

    @Override
    public void recordStepAttempts(String stepId, int attempts) {
        // no-op
    }

    @Override
    public void recordStepOutcome(String stepId, StepOutcome outcome) {
        // no-op
    }

    @Override
    public void recordRetryAttempt(String stepId, int attemptNumber) {
        // no-op
    }
}
