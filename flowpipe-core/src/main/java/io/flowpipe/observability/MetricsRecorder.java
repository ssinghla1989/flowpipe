package io.flowpipe.observability;

public interface MetricsRecorder {

    void recordStepDuration(String stepId, long durationNanos);

    void recordStepAttempts(String stepId, int attempts);

    void recordStepOutcome(String stepId, StepOutcome outcome);

    void recordRetryAttempt(String stepId, int attemptNumber);
}
