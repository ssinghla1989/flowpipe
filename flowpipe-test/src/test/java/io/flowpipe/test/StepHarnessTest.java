package io.flowpipe.test;

import io.flowpipe.api.Step;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StepHarnessTest {

    @Test
    void invokes_identity_step_and_returns_input() {
        Step<String, String> identity = Steps.identity("identity", String.class);

        var outcome = StepHarness.forStep().invoke(identity, "hello");

        assertThat(outcome.succeeded()).isTrue();
        assertThat(outcome.value()).isEqualTo("hello");
    }

    @Test
    void captures_thrown_exception_in_outcome() {
        RuntimeException boom = new RuntimeException("boom");
        Step<String, String> bad = Steps.throwing("bad", String.class, boom);

        var outcome = StepHarness.forStep().invoke(bad, "hi");

        assertThat(outcome.succeeded()).isFalse();
        assertThat(outcome.error()).isSameAs(boom);
    }
}
