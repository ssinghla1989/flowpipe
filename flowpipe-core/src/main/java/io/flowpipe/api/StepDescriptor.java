package io.flowpipe.api;

import io.flowpipe.state.StateKey;
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
    TimeoutPolicy timeoutPolicy,
    CircuitBreakerPolicy circuitBreakerPolicy,  // nullable; null means no circuit breaker active
    StateKey<O> outputKey                        // nullable; non-null means auto-write output to state after execution
) {

    public StepDescriptor {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(inputType, "inputType");
        Objects.requireNonNull(outputType, "outputType");
        Objects.requireNonNull(inputValidator, "inputValidator");
        Objects.requireNonNull(outputValidator, "outputValidator");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        Objects.requireNonNull(timeoutPolicy, "timeoutPolicy");
        // circuitBreakerPolicy and outputKey are intentionally nullable
        if (id.isEmpty()) {
            throw new IllegalArgumentException("StepDescriptor id must not be empty");
        }
    }

    public StepDescriptor<I, O> withRetry(RetryPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator,
            policy, timeoutPolicy, circuitBreakerPolicy, outputKey);
    }

    public StepDescriptor<I, O> withTimeout(TimeoutPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator,
            retryPolicy, policy, circuitBreakerPolicy, outputKey);
    }

    public StepDescriptor<I, O> withCircuitBreaker(CircuitBreakerPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator,
            retryPolicy, timeoutPolicy, policy, outputKey);
    }

    public StepDescriptor<I, O> withOutputKey(StateKey<O> key) {
        Objects.requireNonNull(key, "key");
        return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator,
            retryPolicy, timeoutPolicy, circuitBreakerPolicy, key);
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
        private CircuitBreakerPolicy circuitBreakerPolicy = null;
        private StateKey<O> outputKey = null;

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

        public Builder<I, O> withCircuitBreaker(CircuitBreakerPolicy policy) {
            this.circuitBreakerPolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public Builder<I, O> withOutputKey(StateKey<O> key) {
            this.outputKey = Objects.requireNonNull(key, "key");
            return this;
        }

        public StepDescriptor<I, O> build() {
            return new StepDescriptor<>(id, inputType, outputType, inputValidator, outputValidator,
                retryPolicy, timeoutPolicy, circuitBreakerPolicy, outputKey);
        }
    }
}
