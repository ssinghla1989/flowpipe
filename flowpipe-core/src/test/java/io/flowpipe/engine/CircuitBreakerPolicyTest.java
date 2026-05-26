package io.flowpipe.engine;

import io.flowpipe.api.CircuitBreakerPolicy;
import io.flowpipe.api.StepDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CircuitBreakerPolicyTest {

    // ── 4.1 CircuitBreakerPolicy.of(...) validation ───────────────────────────

    @Test
    void of_creates_policy_with_correct_fields() {
        CircuitBreakerPolicy p = CircuitBreakerPolicy.of(50, 5, 10, 60_000L, 2);
        assertThat(p.failureRateThreshold()).isEqualTo(50);
        assertThat(p.minimumCalls()).isEqualTo(5);
        assertThat(p.slidingWindowSize()).isEqualTo(10);
        assertThat(p.openWindowMs()).isEqualTo(60_000L);
        assertThat(p.halfOpenProbeCount()).isEqualTo(2);
    }

    @Test
    void of_rejects_failure_rate_threshold_below_1() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(0, 5, 10, 60_000L, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_failure_rate_threshold_above_100() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(101, 5, 10, 60_000L, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_minimum_calls_below_1() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(50, 0, 10, 60_000L, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_sliding_window_size_less_than_minimum_calls() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(50, 5, 4, 60_000L, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_negative_open_window_ms() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(50, 5, 10, -1L, 2))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_half_open_probe_count_below_1() {
        assertThatThrownBy(() -> CircuitBreakerPolicy.of(50, 5, 10, 60_000L, 0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_allows_open_window_ms_of_zero() {
        CircuitBreakerPolicy p = CircuitBreakerPolicy.of(50, 1, 1, 0L, 1);
        assertThat(p.openWindowMs()).isEqualTo(0L);
    }

    // ── 4.2 CircuitBreakerPolicy.defaults() ──────────────────────────────────

    @Test
    void defaults_returns_expected_values() {
        CircuitBreakerPolicy p = CircuitBreakerPolicy.defaults();
        assertThat(p.failureRateThreshold()).isEqualTo(50);
        assertThat(p.minimumCalls()).isEqualTo(5);
        assertThat(p.slidingWindowSize()).isEqualTo(10);
        assertThat(p.openWindowMs()).isEqualTo(60_000L);
        assertThat(p.halfOpenProbeCount()).isEqualTo(2);
    }

    // ── 4.3 StepDescriptor.withCircuitBreaker(...) ───────────────────────────

    @Test
    void step_descriptor_default_circuit_breaker_policy_is_null() {
        StepDescriptor<Integer, Integer> desc =
            StepDescriptor.builder("s", Integer.class, Integer.class).build();
        assertThat(desc.circuitBreakerPolicy()).isNull();
    }

    @Test
    void step_descriptor_with_circuit_breaker_round_trips() {
        CircuitBreakerPolicy policy = CircuitBreakerPolicy.defaults();
        StepDescriptor<Integer, Integer> desc =
            StepDescriptor.builder("s", Integer.class, Integer.class)
                .withCircuitBreaker(policy)
                .build();
        assertThat(desc.circuitBreakerPolicy()).isSameAs(policy);
    }

    @Test
    void step_descriptor_with_circuit_breaker_null_throws_npe() {
        StepDescriptor<Integer, Integer> desc =
            StepDescriptor.builder("s", Integer.class, Integer.class).build();
        assertThatThrownBy(() -> desc.withCircuitBreaker(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void step_descriptor_builder_with_circuit_breaker_null_throws_npe() {
        assertThatThrownBy(() ->
            StepDescriptor.builder("s", Integer.class, Integer.class)
                .withCircuitBreaker(null)
                .build())
            .isInstanceOf(NullPointerException.class);
    }
}
