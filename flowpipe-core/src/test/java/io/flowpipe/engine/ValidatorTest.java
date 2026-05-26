package io.flowpipe.engine;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.Success;
import io.flowpipe.validation.ValidationException;
import io.flowpipe.validation.Validator;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatorTest {

    // -------------------------------------------------------------------------
    // Input validation
    // -------------------------------------------------------------------------

    @Test
    void failing_input_validator_surfaces_as_failure_with_validation_exception() {
        ValidationException expected = new ValidationException("blank input not allowed");
        Step<String, String> step = stepWith(
            value -> { if (value.isBlank()) throw expected; },
            null);

        Result<String> result = pipeline(step).execute("   ");

        assertThat(result).isInstanceOf(Failure.class);
        Failure<String> failure = (Failure<String>) result;
        assertThat(failure.cause()).isSameAs(expected);
        assertThat(failure.failedStepId()).isEqualTo("validated");
    }

    @Test
    void input_validator_blocks_step_execution_when_invalid() {
        AtomicBoolean executed = new AtomicBoolean(false);
        Step<String, String> step = new Step<>() {
            private final StepDescriptor<String, String> desc = StepDescriptor
                .builder("guarded", String.class, String.class)
                .inputValidator(v -> { throw new ValidationException("always invalid"); })
                .build();

            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) {
                executed.set(true);
                return input;
            }
        };

        pipeline(step).execute("anything");

        assertThat(executed.get()).isFalse();
    }

    @Test
    void valid_input_passes_through_to_step() {
        Validator<String> requireNonBlank = value -> {
            if (value.isBlank()) throw new ValidationException("blank not allowed");
        };
        Step<String, String> step = stepWith(requireNonBlank, null);

        Result<String> result = pipeline(step).execute("hello");

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<String>) result).value()).isEqualTo("hello");
    }

    // -------------------------------------------------------------------------
    // Output validation
    // -------------------------------------------------------------------------

    @Test
    void failing_output_validator_surfaces_as_failure_with_validation_exception() {
        ValidationException expected = new ValidationException("output too long");
        Step<String, String> step = stepWith(null,
            value -> { if (value.length() > 5) throw expected; });

        Result<String> result = pipeline(step).execute("toolongvalue");

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<String>) result).cause()).isSameAs(expected);
        assertThat(((Failure<String>) result).failedStepId()).isEqualTo("validated");
    }

    @Test
    void output_validator_runs_after_step_has_executed() {
        AtomicInteger callOrder = new AtomicInteger(0);
        AtomicInteger stepOrder = new AtomicInteger(-1);
        AtomicInteger validatorOrder = new AtomicInteger(-1);

        Step<String, String> step = new Step<>() {
            private final StepDescriptor<String, String> desc = StepDescriptor
                .builder("ordering", String.class, String.class)
                .outputValidator(v -> validatorOrder.set(callOrder.incrementAndGet()))
                .build();

            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) {
                stepOrder.set(callOrder.incrementAndGet());
                return input;
            }
        };

        pipeline(step).execute("hello");

        assertThat(stepOrder.get()).isLessThan(validatorOrder.get());
    }

    @Test
    void output_validator_receives_the_value_returned_by_step() {
        AtomicBoolean validatorSawCorrectValue = new AtomicBoolean(false);
        Step<String, String> step = new Step<>() {
            private final StepDescriptor<String, String> desc = StepDescriptor
                .builder("check-output", String.class, String.class)
                .outputValidator(v -> validatorSawCorrectValue.set("HELLO".equals(v)))
                .build();

            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) {
                return input.toUpperCase();
            }
        };

        pipeline(step).execute("hello");

        assertThat(validatorSawCorrectValue.get()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Both validators in sequence
    // -------------------------------------------------------------------------

    @Test
    void input_and_output_validators_both_run_on_success() {
        AtomicBoolean inputValidated = new AtomicBoolean(false);
        AtomicBoolean outputValidated = new AtomicBoolean(false);

        Step<String, String> step = new Step<>() {
            private final StepDescriptor<String, String> desc = StepDescriptor
                .builder("both", String.class, String.class)
                .inputValidator(v -> inputValidated.set(true))
                .outputValidator(v -> outputValidated.set(true))
                .build();

            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) { return input; }
        };

        pipeline(step).execute("ok");

        assertThat(inputValidated.get()).isTrue();
        assertThat(outputValidated.get()).isTrue();
    }

    @Test
    void output_validator_does_not_run_when_input_validator_fails() {
        AtomicBoolean outputValidatorRan = new AtomicBoolean(false);

        Step<String, String> step = new Step<>() {
            private final StepDescriptor<String, String> desc = StepDescriptor
                .builder("short-circuit", String.class, String.class)
                .inputValidator(v -> { throw new ValidationException("input rejected"); })
                .outputValidator(v -> outputValidatorRan.set(true))
                .build();

            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) { return input; }
        };

        pipeline(step).execute("anything");

        assertThat(outputValidatorRan.get()).isFalse();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static Step<String, String> stepWith(Validator<String> inputValidator,
                                                  Validator<String> outputValidator) {
        StepDescriptor.Builder<String, String> b =
            StepDescriptor.builder("validated", String.class, String.class);
        if (inputValidator != null) b.inputValidator(inputValidator);
        if (outputValidator != null) b.outputValidator(outputValidator);
        StepDescriptor<String, String> desc = b.build();

        return new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, StepContext ctx) { return input; }
        };
    }

    private static Pipeline<String, String> pipeline(Step<String, String> step) {
        return PipelineBuilder.start(String.class).then(step).build();
    }
}
