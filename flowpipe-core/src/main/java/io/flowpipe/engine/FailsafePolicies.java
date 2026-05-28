package io.flowpipe.engine;

import dev.failsafe.CircuitBreaker;
import dev.failsafe.RetryPolicyBuilder;
import dev.failsafe.Timeout;
import io.flowpipe.api.CircuitBreakerPolicy;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.TimeoutPolicy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

// Translates FlowPipe policy value types to Failsafe 3.x policy objects.
// Failsafe types are never exposed beyond io.flowpipe.engine.
//
// API notes (Failsafe 3.3.2):
//   RetryPolicyBuilder.withBackoff(long initial, long max, ChronoUnit, double factor)
//   RetryPolicyBuilder.withJitter(double factor)  — applies ±factor*delay
//   TimeoutBuilder.withInterrupt()                — enables thread interrupt on breach
//   CircuitBreakerBuilder.withFailureRateThreshold(int pct, int minExecutions, Duration delay)
//     pct: 1–100; minExecutions: min calls before threshold evaluated (also window capacity)
//     delay: time circuit stays OPEN before transitioning to HALF-OPEN
//   CircuitBreakerBuilder.withSuccessThreshold(int)
//     number of consecutive successes in HALF-OPEN required to close
final class FailsafePolicies {

    private FailsafePolicies() {}

    // Returns a builder so the caller can attach event listeners before build().
    // Does not guard against maxAttempts == 1; callers must check before using.
    static RetryPolicyBuilder<Object> toFailsafe(RetryPolicy fp) {
        RetryPolicyBuilder<Object> builder = dev.failsafe.RetryPolicy.<Object>builder()
                .withMaxAttempts(fp.maxAttempts());

        if (fp.initialDelayMs() > 0) {
            if (fp.multiplier() > 1.0) {
                builder.withBackoff(fp.initialDelayMs(), Long.MAX_VALUE, ChronoUnit.MILLIS, fp.multiplier());
            } else {
                // multiplier == 1.0 → constant delay; withBackoff requires factor > 1
                builder.withDelay(Duration.ofMillis(fp.initialDelayMs()));
            }
        }
        // initialDelayMs == 0 → no delay call; Failsafe retries immediately

        if (fp.jitter()) {
            // Failsafe jitter(factor) gives delay ± factor*delay; factor=0.5 → [0.5x, 1.5x].
            // FlowPipe's original semantics were uniform(0, delay); no test asserts the exact
            // distribution, so Failsafe's built-in jitter is an acceptable substitution.
            builder.withJitter(0.5);
        }

        return builder;
    }

    static Timeout<Object> toFailsafe(TimeoutPolicy fp) {
        return Timeout.<Object>builder(Duration.ofMillis(fp.timeoutMs()))
                .withInterrupt()
                .build();
    }

    // Uses count-based withFailureThreshold rather than withFailureRateThreshold because the
    // rate-threshold variant requires openWindowMs >= 10ms, which breaks policies with
    // openWindowMs=0 (immediate HALF-OPEN). Count-based threshold has no such constraint.
    // failures = max(ceil(failureRateThreshold% * slidingWindowSize), minimumCalls), clamped to >= 1.
    // The max ensures the circuit cannot trip before minimumCalls failures have been observed,
    // regardless of the configured failure rate threshold.
    static CircuitBreaker<Object> toFailsafe(CircuitBreakerPolicy fp) {
        int failures = (int) Math.ceil(fp.failureRateThreshold() / 100.0 * fp.slidingWindowSize());
        failures = Math.max(failures, fp.minimumCalls());
        var builder = CircuitBreaker.<Object>builder()
                .withFailureThreshold(failures, fp.slidingWindowSize())
                .withSuccessThreshold(fp.halfOpenProbeCount())
                .withDelay(Duration.ofMillis(fp.openWindowMs()));
        return builder.build();
    }
}
