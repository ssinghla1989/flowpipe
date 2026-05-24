package io.flowpipe.api;

import java.util.Objects;

public final class RetryPolicy {

    private final int maxAttempts;
    private final long initialDelayMs;
    private final double multiplier;
    private final boolean jitter;

    private RetryPolicy(int maxAttempts, long initialDelayMs, double multiplier, boolean jitter) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got: " + maxAttempts);
        }
        if (initialDelayMs < 0) {
            throw new IllegalArgumentException("initialDelayMs must be >= 0, got: " + initialDelayMs);
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be >= 1.0, got: " + multiplier);
        }
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.multiplier = multiplier;
        this.jitter = jitter;
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, 0L, 1.0, false);
    }

    public static RetryPolicy fixed(int maxAttempts, long delayMs) {
        return new RetryPolicy(maxAttempts, delayMs, 1.0, false);
    }

    public static RetryPolicy exponential(int maxAttempts, long initialDelayMs, double multiplier, boolean jitter) {
        return new RetryPolicy(maxAttempts, initialDelayMs, multiplier, jitter);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public long initialDelayMs() {
        return initialDelayMs;
    }

    public double multiplier() {
        return multiplier;
    }

    public boolean jitter() {
        return jitter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetryPolicy r)) return false;
        return maxAttempts == r.maxAttempts
            && initialDelayMs == r.initialDelayMs
            && Double.compare(multiplier, r.multiplier) == 0
            && jitter == r.jitter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxAttempts, initialDelayMs, multiplier, jitter);
    }

    @Override
    public String toString() {
        return "RetryPolicy{maxAttempts=" + maxAttempts + ", initialDelayMs=" + initialDelayMs
            + ", multiplier=" + multiplier + ", jitter=" + jitter + "}";
    }
}
