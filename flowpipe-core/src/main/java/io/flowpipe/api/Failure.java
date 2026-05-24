package io.flowpipe.api;

import java.util.Objects;

public record Failure<O>(Throwable cause, String failedStepId, ExecutionTrace trace) implements Result<O> {

    public Failure {
        Objects.requireNonNull(cause, "cause");
        Objects.requireNonNull(failedStepId, "failedStepId");
        Objects.requireNonNull(trace, "trace");
    }
}
