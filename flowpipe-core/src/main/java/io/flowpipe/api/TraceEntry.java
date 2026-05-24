package io.flowpipe.api;

import java.util.Objects;

public record TraceEntry(
    String stepId,
    long startedAtNanos,
    long durationNanos,
    int attempts,
    boolean skipped
) {

    public TraceEntry {
        Objects.requireNonNull(stepId, "stepId");
        if (skipped) {
            if (attempts != 0) {
                throw new IllegalArgumentException("attempts must be 0 for skipped entries");
            }
            if (durationNanos != 0) {
                throw new IllegalArgumentException("durationNanos must be 0 for skipped entries");
            }
        } else {
            if (attempts < 1) {
                throw new IllegalArgumentException("attempts must be >= 1");
            }
            if (durationNanos < 0) {
                throw new IllegalArgumentException("durationNanos must be >= 0");
            }
        }
    }
}
