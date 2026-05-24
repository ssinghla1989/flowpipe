package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.api.StepContext;
import io.flowpipe.validation.ValidationException;
import io.flowpipe.validation.Validator;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class StepExecutionTest {

    @Test
    void input_validation_failure_prevents_execute_and_yields_failure() {
        AtomicBoolean executed = new AtomicBoolean(false);
        Validator<String> reject = v -> { throw new ValidationException("nope"); };
        StepDescriptor<String, String> desc = StepDescriptor.builder("validate", String.class, String.class)
            .inputValidator(reject)
            .build();
        Step<String, String> step = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) {
                executed.set(true);
                return input;
            }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(step).build();
        Result<String> result = pipeline.execute("hi");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("validate");
        assertThat(failure.cause()).isInstanceOf(ValidationException.class);
        assertThat(executed.get()).isFalse();
    }

    @Test
    void output_validation_failure_halts_pipeline_and_skips_subsequent_steps() {
        Validator<String> rejectAll = v -> { throw new ValidationException("bad output"); };
        StepDescriptor<String, String> firstDesc = StepDescriptor.builder("first", String.class, String.class)
            .outputValidator(rejectAll)
            .build();
        Step<String, String> first = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return firstDesc; }
            @Override public String execute(String input, StepContext ctx) { return input; }
        };
        AtomicBoolean secondRan = new AtomicBoolean(false);
        Step<String, String> second = Step.of("second", String.class, String.class,
            (s, ctx) -> { secondRan.set(true); return s; });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(first).then(second).build();
        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("first");
        assertThat(secondRan.get()).isFalse();
    }

    @Test
    void noop_validator_defaults_allow_null_and_any_value() {
        Step<String, String> nullable = Step.of("nullable", String.class, String.class,
            (s, ctx) -> null);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(nullable).build();
        Result<String> result = pipeline.execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isNull();
    }

    @Test
    void step_execute_exceptions_surface_as_failure_with_cause_and_id() {
        RuntimeException boom = new RuntimeException("boom");
        Step<String, String> bad = Step.of("bad", String.class, String.class,
            (s, ctx) -> { throw boom; });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(bad).build();
        Result<String> result = pipeline.execute("x");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.failedStepId()).isEqualTo("bad");
        assertThat(failure.cause()).isSameAs(boom);
    }

    @Test
    void step_context_state_and_context_are_non_null_with_empty_context() {
        AtomicBoolean stateNotNull = new AtomicBoolean(false);
        AtomicBoolean contextNotNull = new AtomicBoolean(false);
        Step<String, String> inspector = Step.of("inspector", String.class, String.class,
            (s, ctx) -> {
                stateNotNull.set(ctx.state() != null);
                contextNotNull.set(ctx.context() != null);
                return s;
            });

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(inspector).build();
        pipeline.execute("x");

        assertThat(stateNotNull.get()).isTrue();
        assertThat(contextNotNull.get()).isTrue();
    }
}
