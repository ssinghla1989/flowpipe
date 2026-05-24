package io.flowpipe.api;

import java.util.Objects;
import java.util.function.BiFunction;

public interface Step<I, O> {

    StepDescriptor<I, O> describe();

    O execute(I input, StepContext ctx) throws Exception;

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
