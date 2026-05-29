package io.flowpipe.api;

import io.flowpipe.state.StateKey;

import java.util.Objects;
import java.util.function.BiFunction;

public interface Step<I, O> {

    StepDescriptor<I, O> describe();

    O execute(I input, StepContext ctx) throws Exception;

    default Step<I, O> withRetry(RetryPolicy policy) {
        StepDescriptor<I, O> desc = describe().withRetry(policy);
        Step<I, O> self = this;
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return desc; }
            @Override public O execute(I input, StepContext ctx) throws Exception { return self.execute(input, ctx); }
        };
    }

    default Step<I, O> withTimeout(TimeoutPolicy policy) {
        StepDescriptor<I, O> desc = describe().withTimeout(policy);
        Step<I, O> self = this;
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return desc; }
            @Override public O execute(I input, StepContext ctx) throws Exception { return self.execute(input, ctx); }
        };
    }

    default Step<I, O> withCircuitBreaker(CircuitBreakerPolicy policy) {
        StepDescriptor<I, O> desc = describe().withCircuitBreaker(policy);
        Step<I, O> self = this;
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return desc; }
            @Override public O execute(I input, StepContext ctx) throws Exception { return self.execute(input, ctx); }
        };
    }

    default Step<I, O> withOutputKey(StateKey<O> key) {
        StepDescriptor<I, O> desc = describe().withOutputKey(key);
        Step<I, O> self = this;
        return new Step<>() {
            @Override public StepDescriptor<I, O> describe() { return desc; }
            @Override public O execute(I input, StepContext ctx) throws Exception { return self.execute(input, ctx); }
        };
    }

    static <I, O> Step<I, O> of(String id,
                                Class<I> inputType,
                                Class<O> outputType,
                                BiFunction<I, StepContext, O> body) {
        Objects.requireNonNull(body, "body");
        StepDescriptor<I, O> descriptor =
            StepDescriptor.builder(id, inputType, outputType).build();
        return new Step<>() {
            @Override
            public StepDescriptor<I, O> describe() {
                return descriptor;
            }

            @Override
            public O execute(I input, StepContext ctx) {
                return body.apply(input, ctx);
            }
        };
    }
}
