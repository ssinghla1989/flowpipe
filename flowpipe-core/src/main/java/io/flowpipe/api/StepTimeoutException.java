package io.flowpipe.api;

public final class StepTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String stepId;
    private final long timeoutMs;

    public StepTimeoutException(String stepId, long timeoutMs) {
        super("Step '" + stepId + "' exceeded timeout of " + timeoutMs + "ms");
        this.stepId = stepId;
        this.timeoutMs = timeoutMs;
    }

    public String stepId() {
        return stepId;
    }

    public long timeoutMs() {
        return timeoutMs;
    }
}
