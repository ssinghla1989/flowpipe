package io.flowpipe.api;

public class PipelineDeadlineExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long deadlineMs;

    public PipelineDeadlineExceededException(long deadlineMs) {
        super("Pipeline deadline of " + deadlineMs + " ms exceeded");
        this.deadlineMs = deadlineMs;
    }

    public long deadlineMs() {
        return deadlineMs;
    }
}
