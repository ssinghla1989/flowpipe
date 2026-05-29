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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void noop_validator_accepts_any_non_null_value_including_empty_string() {
        Step<String, String> passThrough = Step.of("pass", String.class, String.class,
            (s, ctx) -> s.isEmpty() ? "empty" : s);

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(passThrough).build();

        assertThat(pipeline.execute("hello")).isInstanceOf(Success.class);
        assertThat(((Success<String>) pipeline.execute("")).value()).isEqualTo("empty");
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
    void execute_with_null_input_throws_before_any_step_runs() {
        AtomicBoolean stepRan = new AtomicBoolean(false);
        Step<String, String> step = Step.of("s", String.class, String.class, (s, ctx) -> {
            stepRan.set(true);
            return s;
        });
        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(step).build();

        assertThatThrownBy(() -> pipeline.execute(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("input");
        assertThat(stepRan.get()).isFalse();
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
