package io.flowpipe.api;

import java.util.Objects;
import java.util.function.Predicate;

public final class RetryPolicy {

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final boolean jitter;
    // null means "retry on any throwable" (default Failsafe behaviour)
    private final Predicate<Throwable> retryPredicate;

    private RetryPolicy(int maxAttempts, long initialDelayMs, long maxDelayMs, double multiplier, boolean jitter,
                        Predicate<Throwable> retryPredicate) {
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
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        this.jitter = jitter;
        this.retryPredicate = retryPredicate;
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, 0L, 0L, 1.0, false, null);
    }

    public static RetryPolicy fixed(int maxAttempts, long delayMs) {
        return new RetryPolicy(maxAttempts, delayMs, delayMs, 1.0, false, null);
    }

    /**
     * Exponential backoff. {@code initialDelayMs} is the delay before the second attempt;
     * each subsequent attempt multiplies the previous delay by {@code multiplier}, capped at
     * {@code maxDelayMs}. Both {@code initialDelayMs} and {@code maxDelayMs} must be positive,
     * and {@code maxDelayMs} must be >= {@code initialDelayMs}.
     */
    public static RetryPolicy exponential(int maxAttempts, long initialDelayMs, long maxDelayMs,
                                          double multiplier, boolean jitter) {
        if (initialDelayMs <= 0) {
            throw new IllegalArgumentException(
                "initialDelayMs must be > 0 for exponential backoff, got: " + initialDelayMs
                    + " — use RetryPolicy.fixed() for no-delay retries");
        }
        if (maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException(
                "maxDelayMs must be >= initialDelayMs, got: maxDelayMs=" + maxDelayMs
                    + ", initialDelayMs=" + initialDelayMs);
        }
        return new RetryPolicy(maxAttempts, initialDelayMs, maxDelayMs, multiplier, jitter, null);
    }

    /**
     * Returns a new {@code RetryPolicy} that only retries when {@code predicate} returns
     * {@code true} for the thrown exception. When the predicate returns {@code false}, the
     * exception propagates immediately — no further attempts are made and the pipeline
     * produces a {@link Failure} with that exception as the cause.
     *
     * <p>The predicate is evaluated on the raw throwable before any Failsafe unwrapping.
     * Both checked and unchecked exceptions are passed to the predicate as-is.
     *
     * <p>By default (when this method is not called), the policy retries on any
     * {@link Throwable}.</p>
     *
     * @param predicate returns {@code true} if the exception warrants a retry
     * @return a new {@code RetryPolicy} with the predicate applied; the original is unchanged
     */
    public RetryPolicy retryOn(Predicate<Throwable> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return new RetryPolicy(maxAttempts, initialDelayMs, maxDelayMs, multiplier, jitter, predicate);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public long initialDelayMs() {
        return initialDelayMs;
    }

    public long maxDelayMs() {
        return maxDelayMs;
    }

    public double multiplier() {
        return multiplier;
    }

    public boolean jitter() {
        return jitter;
    }

    /**
     * Returns the predicate that gates retries, or {@code null} if the policy retries on
     * any throwable (the default).
     */
    public Predicate<Throwable> retryPredicate() {
        return retryPredicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetryPolicy r)) return false;
        // retryPredicate excluded: lambdas have no value equality
        return maxAttempts == r.maxAttempts
            && initialDelayMs == r.initialDelayMs
            && maxDelayMs == r.maxDelayMs
            && Double.compare(multiplier, r.multiplier) == 0
            && jitter == r.jitter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxAttempts, initialDelayMs, maxDelayMs, multiplier, jitter);
    }

    @Override
    public String toString() {
        return "RetryPolicy{maxAttempts=" + maxAttempts + ", initialDelayMs=" + initialDelayMs
            + ", maxDelayMs=" + maxDelayMs + ", multiplier=" + multiplier + ", jitter=" + jitter
            + ", retryPredicate=" + (retryPredicate == null ? "any" : "custom") + "}";
    }
}
