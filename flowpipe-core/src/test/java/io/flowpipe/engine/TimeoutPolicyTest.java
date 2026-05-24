package io.flowpipe.engine;

import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.TimeoutPolicy;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeoutPolicyTest {

    // ── 4.1 TimeoutPolicy.none() ─────────────────────────────────────────────

    @Test
    void none_has_timeout_ms_zero() {
        assertThat(TimeoutPolicy.none().timeoutMs()).isEqualTo(0L);
    }

    // ── 4.2 TimeoutPolicy.ofMillis ───────────────────────────────────────────

    @Test
    void ofMillis_stores_correct_deadline() {
        assertThat(TimeoutPolicy.ofMillis(500).timeoutMs()).isEqualTo(500L);
    }

    // ── 4.3 TimeoutPolicy.of converts to milliseconds ────────────────────────

    @Test
    void of_converts_duration_and_unit_to_milliseconds() {
        assertThat(TimeoutPolicy.of(2, TimeUnit.SECONDS).timeoutMs()).isEqualTo(2000L);
    }

    // ── 4.4 ofMillis rejects zero and negative ────────────────────────────────

    @Test
    void ofMillis_rejects_zero() {
        assertThatThrownBy(() -> TimeoutPolicy.ofMillis(0))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ofMillis_rejects_negative() {
        assertThatThrownBy(() -> TimeoutPolicy.ofMillis(-1))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // ── 4.5 of rejects invalid duration / null unit ──────────────────────────

    @Test
    void of_rejects_zero_duration() {
        assertThatThrownBy(() -> TimeoutPolicy.of(0, TimeUnit.SECONDS))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_rejects_null_unit() {
        assertThatThrownBy(() -> TimeoutPolicy.of(1, null))
            .isInstanceOf(NullPointerException.class);
    }

    // ── 5.1 StepDescriptor default timeoutPolicy ─────────────────────────────

    @Test
    void descriptor_default_timeout_policy_is_none() {
        StepDescriptor<String, String> d = StepDescriptor
            .builder("step", String.class, String.class)
            .build();
        assertThat(d.timeoutPolicy().timeoutMs()).isEqualTo(0L);
    }

    // ── 5.2 withTimeout on descriptor ────────────────────────────────────────

    @Test
    void with_timeout_returns_descriptor_with_updated_policy() {
        StepDescriptor<String, String> d = StepDescriptor
            .builder("step", String.class, String.class)
            .build()
            .withTimeout(TimeoutPolicy.ofMillis(300));
        assertThat(d.timeoutPolicy().timeoutMs()).isEqualTo(300L);
    }

    // ── 5.3 withTimeout(null) throws NullPointerException ────────────────────

    @Test
    void with_timeout_null_throws_npe() {
        StepDescriptor<String, String> d = StepDescriptor
            .builder("step", String.class, String.class)
            .build();
        assertThatThrownBy(() -> d.withTimeout(null))
            .isInstanceOf(NullPointerException.class);
    }

    // ── 5.4 Builder.withTimeout ───────────────────────────────────────────────

    @Test
    void builder_with_timeout_sets_policy() {
        StepDescriptor<String, String> d = StepDescriptor
            .builder("step", String.class, String.class)
            .withTimeout(TimeoutPolicy.ofMillis(100))
            .build();
        assertThat(d.timeoutPolicy().timeoutMs()).isEqualTo(100L);
    }
}
