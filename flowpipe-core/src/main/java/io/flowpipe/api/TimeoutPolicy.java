package io.flowpipe.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class TimeoutPolicy {

    private final long timeoutMs;

    private TimeoutPolicy(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public static TimeoutPolicy none() {
        return new TimeoutPolicy(0);
    }

    public static TimeoutPolicy ofMillis(long ms) {
        if (ms < 1) {
            throw new IllegalArgumentException("TimeoutPolicy ms must be >= 1, got: " + ms);
        }
        return new TimeoutPolicy(ms);
    }

    public static TimeoutPolicy of(long duration, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        if (duration < 1) {
            throw new IllegalArgumentException("TimeoutPolicy duration must be >= 1, got: " + duration);
        }
        return new TimeoutPolicy(unit.toMillis(duration));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeoutPolicy other)) return false;
        return timeoutMs == other.timeoutMs;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(timeoutMs);
    }

    @Override
    public String toString() {
        return timeoutMs == 0 ? "TimeoutPolicy.none()" : "TimeoutPolicy{timeoutMs=" + timeoutMs + "}";
    }
}
