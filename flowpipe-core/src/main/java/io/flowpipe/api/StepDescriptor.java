package io.flowpipe.api;

import io.flowpipe.validation.NoOpValidator;
import io.flowpipe.validation.Validator;

import java.util.Objects;

public record StepDescriptor<I, O>(
    String id,
    Class<I> inputType,
    Class<O> outputType,
    Validator<I> inputValidator,
    Validator<O> outputValidator,
    RetryPolicy retryPolicy,
    TimeoutPolicy timeoutPolicy
) {

    public StepDescriptor {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(inputType, "inputType");
        Objects.requireNonNull(outputType, "outputType");
        Objects.requireNonNull(inputValidator, "inputValidator");
        Objects.requireNonNull(outputValidator, "outputValidator");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        Objects.requireNonNull(timeoutPolicy, "timeoutPolicy");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("StepDescriptor id must not be empty");
        }
    }

    public StepDescriptor<I, O> withRetry(RetryPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator, policy, timeoutPolicy);
    }

    public StepDescriptor<I, O> withTimeout(TimeoutPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator, retryPolicy, policy);
    }

    public static <I, O> Builder<I, O> builder(String id, Class<I> inputType, Class<O> outputType) {
        return new Builder<>(id, inputType, outputType);
    }

    public static final class Builder<I, O> {
        private final String id;
        private final Class<I> inputType;
        private final Class<O> outputType;
        private Validator<I> inputValidator = NoOpValidator.instance();
        private Validator<O> outputValidator = NoOpValidator.instance();
        private RetryPolicy retryPolicy = RetryPolicy.none();
        private TimeoutPolicy timeoutPolicy = TimeoutPolicy.none();

        private Builder(String id, Class<I> inputType, Class<O> outputType) {
            this.id = id;
            this.inputType = inputType;
            this.outputType = outputType;
        }

        public Builder<I, O> inputValidator(Validator<I> validator) {
            this.inputValidator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        public Builder<I, O> outputValidator(Validator<O> validator) {
            this.outputValidator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        public Builder<I, O> withRetry(RetryPolicy policy) {
            this.retryPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public Builder<I, O> withTimeout(TimeoutPolicy policy) {
            this.timeoutPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public StepDescriptor<I, O> build() {
            return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator, retryPolicy, timeoutPolicy);
        }
    }
}
