package io.flowpipe.api;

import java.util.Objects;

public record Success<O>(O value, ExecutionTrace trace) implements Result<O> {

    public Success {
        Objects.requireNonNull(trace, "trace");
    }
}
