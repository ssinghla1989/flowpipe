package io.flowpipe.api;

public interface PipelineLifecycle<I, O> {

    default void onStart(I input, StepContext ctx) {}

    default void onFinish(Result<O> result, StepContext ctx) {}

    default void onError(Failure<O> failure, StepContext ctx) {}
}
