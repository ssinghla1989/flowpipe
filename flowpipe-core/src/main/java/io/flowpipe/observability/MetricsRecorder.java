package io.flowpipe.observability;

public interface MetricsRecorder {

    void recordStepDuration(String stepId, long durationNanos);

    void recordStepAttempts(String stepId, int attempts);

    void recordStepOutcome(String stepId, StepOutcome outcome);

    void recordRetryAttempt(String stepId, int attemptNumber);

    /**
     * Records the total wall-clock duration of one {@code pipeline.execute(...)} call, from
     * the moment input validation succeeds through the final result (success or failure).
     *
     * <p>Implementations typically expose this as a histogram tagged by pipeline outcome —
     * letting consumers compute pipeline-level P99 latency, success rate, and throughput
     * without joining per-step metrics in the backend.
     *
     * <p>Default is no-op so existing recorders compile unchanged; override to opt in.
     *
     * @param durationNanos total wall-clock execution time in nanoseconds
     * @param outcome       {@link StepOutcome#SUCCESS} for a {@code Success} result;
     *                      {@link StepOutcome#FAILURE} for any {@code Failure}
     */
    default void recordPipelineDuration(long durationNanos, StepOutcome outcome) {
        // no-op
    }

    /**
     * Records a single pipeline execution outcome (success or failure). Suitable for backends
     * that prefer a separate counter from the duration histogram.
     *
     * <p>Default is no-op so existing recorders compile unchanged; override to opt in.
     */
    default void recordPipelineOutcome(StepOutcome outcome) {
        // no-op
    }
}
