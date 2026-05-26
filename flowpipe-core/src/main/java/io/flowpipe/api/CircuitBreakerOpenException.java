package io.flowpipe.api;

import java.time.Instant;

public final class CircuitBreakerOpenException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String stepId;
    private final Instant retriableAfter;

    public CircuitBreakerOpenException(String stepId, Instant retriableAfter) {
        super("Circuit breaker for step '" + stepId + "' is OPEN; earliest retry after: " + retriableAfter);
        this.stepId = stepId;
        this.retriableAfter = retriableAfter;
    }

    public String stepId() { return stepId; }
    public Instant retriableAfter() { return retriableAfter; }
}
