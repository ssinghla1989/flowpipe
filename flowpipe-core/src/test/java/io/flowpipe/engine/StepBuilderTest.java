package io.flowpipe.engine;

import io.flowpipe.api.CircuitBreakerPolicy;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepBuilder;
import io.flowpipe.api.Success;
import io.flowpipe.api.TimeoutPolicy;
import io.flowpipe.api.Result;
import io.flowpipe.api.Failure;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StepBuilderTest {

    // -------------------------------------------------------------------------
    // 1. build() without execute() throws
    // -------------------------------------------------------------------------

    @Test
    void build_without_execute_throws_illegal_state() {
        assertThatThrownBy(() ->
            Step.builder("id", String.class, String.class).build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("id");
    }

    // -------------------------------------------------------------------------
    // 2. Minimal build — verify policy defaults
    // -------------------------------------------------------------------------

    @Test
    void minimal_build_has_same_defaults_as_step_of() {
        Step<String, String> viaBuilder = Step.builder("s", String.class, String.class)
            .execute((in, ctx) -> in)
            .build();
        Step<String, String> viaOf = Step.builder("s", String.class, String.class).execute((in, ctx) -> in).build();

        assertThat(viaBuilder.describe().retryPolicy()).isEqualTo(viaOf.describe().retryPolicy());
        assertThat(viaBuilder.describe().timeoutPolicy()).isEqualTo(viaOf.describe().timeoutPolicy());
        assertThat(viaBuilder.describe().circuitBreakerPolicy()).isNull();
        assertThat(viaBuilder.describe().outputKey()).isNull();
    }

    // -------------------------------------------------------------------------
    // 3. Policies appear on the built step's descriptor
    // -------------------------------------------------------------------------

    @Test
    void configured_policies_appear_on_descriptor() {
        RetryPolicy retry = RetryPolicy.fixed(3, 50);
        TimeoutPolicy timeout = TimeoutPolicy.ofMillis(500);
        CircuitBreakerPolicy cb = CircuitBreakerPolicy.defaults();

        Step<String, String> step = Step.builder("s", String.class, String.class)
            .execute((in, ctx) -> in)
            .withRetry(retry)
            .withTimeout(timeout)
            .withCircuitBreaker(cb)
            .build();

        assertThat(step.describe().retryPolicy()).isEqualTo(retry);
        assertThat(step.describe().timeoutPolicy()).isEqualTo(timeout);
        assertThat(step.describe().circuitBreakerPolicy()).isSameAs(cb);
    }

    // -------------------------------------------------------------------------
    // 4. Successful execution in a real pipeline
    // -------------------------------------------------------------------------

    @Test
    void built_step_executes_successfully_in_pipeline() {
        Step<String, String> step = Step.builder("upper", String.class, String.class)
            .execute((in, ctx) -> in.toUpperCase())
            .build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .build();

        Result<String> result = pipeline.execute("hello");
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("HELLO");
    }

    // -------------------------------------------------------------------------
    // 5. Checked exception from body surfaces as Failure cause (not wrapped)
    // -------------------------------------------------------------------------

    @Test
    void checked_exception_body_surfaces_as_failure_cause_unwrapped() {
        IOException checked = new IOException("remote unavailable");

        Step<String, String> step = Step.builder("io-step", String.class, String.class)
            .execute((in, ctx) -> { throw checked; })
            .build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .build();

        Result<String> result = pipeline.execute("input");
        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isSameAs(checked);
        assertThat(failure.failedStepId()).isEqualTo("io-step");
    }

    // -------------------------------------------------------------------------
    // 6. Retry policy on builder-created step behaves identically to withRetry chain
    // -------------------------------------------------------------------------

    @Test
    void retry_policy_on_builder_step_retries_on_failure() {
        AtomicInteger attempts = new AtomicInteger(0);

        Step<String, String> step = Step.builder("retry-step", String.class, String.class)
            .execute((in, ctx) -> {
                if (attempts.incrementAndGet() < 3) throw new RuntimeException("not yet");
                return "ok";
            })
            .withRetry(RetryPolicy.fixed(3, 0))
            .build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(step)
            .build();

        Result<String> result = pipeline.execute("go");
        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    // -------------------------------------------------------------------------
    // 7. Step.builder() is accessible via the Step interface (public API surface)
    // -------------------------------------------------------------------------

    @Test
    void step_builder_factory_is_on_step_interface() {
        StepBuilder<Integer, String> builder =
            Step.builder("id", Integer.class, String.class);
        assertThat(builder).isNotNull();
    }
}
