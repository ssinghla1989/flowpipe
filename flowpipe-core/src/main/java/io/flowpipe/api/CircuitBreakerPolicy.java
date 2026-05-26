package io.flowpipe.api;

public final class CircuitBreakerPolicy {

    private final int failureRateThreshold;
    private final int minimumCalls;
    private final int slidingWindowSize;
    private final long openWindowMs;
    private final int halfOpenProbeCount;

    private CircuitBreakerPolicy(int failureRateThreshold, int minimumCalls,
                                  int slidingWindowSize, long openWindowMs,
                                  int halfOpenProbeCount) {
        this.failureRateThreshold = failureRateThreshold;
        this.minimumCalls = minimumCalls;
        this.slidingWindowSize = slidingWindowSize;
        this.openWindowMs = openWindowMs;
        this.halfOpenProbeCount = halfOpenProbeCount;
    }

    public static CircuitBreakerPolicy of(int failureRateThreshold, int minimumCalls,
                                          int slidingWindowSize, long openWindowMs,
                                          int halfOpenProbeCount) {
        if (failureRateThreshold < 1 || failureRateThreshold > 100) {
            throw new IllegalArgumentException(
                "failureRateThreshold must be 1–100, got: " + failureRateThreshold);
        }
        if (minimumCalls < 1) {
            throw new IllegalArgumentException(
                "minimumCalls must be >= 1, got: " + minimumCalls);
        }
        if (slidingWindowSize < minimumCalls) {
            throw new IllegalArgumentException(
                "slidingWindowSize must be >= minimumCalls (" + minimumCalls + "), got: " + slidingWindowSize);
        }
        if (openWindowMs < 0) {
            throw new IllegalArgumentException(
                "openWindowMs must be >= 0, got: " + openWindowMs);
        }
        if (halfOpenProbeCount < 1) {
            throw new IllegalArgumentException(
                "halfOpenProbeCount must be >= 1, got: " + halfOpenProbeCount);
        }
        return new CircuitBreakerPolicy(failureRateThreshold, minimumCalls,
            slidingWindowSize, openWindowMs, halfOpenProbeCount);
    }

    public static CircuitBreakerPolicy defaults() {
        return new CircuitBreakerPolicy(50, 5, 10, 60_000L, 2);
    }

    public int failureRateThreshold() { return failureRateThreshold; }
    public int minimumCalls() { return minimumCalls; }
    public int slidingWindowSize() { return slidingWindowSize; }
    public long openWindowMs() { return openWindowMs; }
    public int halfOpenProbeCount() { return halfOpenProbeCount; }

    @Override
    public String toString() {
        return "CircuitBreakerPolicy{failureRateThreshold=" + failureRateThreshold
            + ", minimumCalls=" + minimumCalls
            + ", slidingWindowSize=" + slidingWindowSize
            + ", openWindowMs=" + openWindowMs
            + ", halfOpenProbeCount=" + halfOpenProbeCount + "}";
    }
}
