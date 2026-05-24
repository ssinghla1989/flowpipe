package io.flowpipe.test;

import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.StepContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RecordingPipelineLifecycle<I, O> implements PipelineLifecycle<I, O> {

    public record StartInvocation<I>(I input, StepContext ctx) {}

    public record FinishInvocation<O>(Result<O> result, StepContext ctx) {}

    public record ErrorInvocation<O>(Failure<O> failure, StepContext ctx) {}

    private final List<StartInvocation<I>> startInvocations = new ArrayList<>();
    private final List<FinishInvocation<O>> finishInvocations = new ArrayList<>();
    private final List<ErrorInvocation<O>> errorInvocations = new ArrayList<>();

    @Override
    public synchronized void onStart(I input, StepContext ctx) {
        startInvocations.add(new StartInvocation<>(input, ctx));
    }

    @Override
    public synchronized void onFinish(Result<O> result, StepContext ctx) {
        finishInvocations.add(new FinishInvocation<>(result, ctx));
    }

    @Override
    public synchronized void onError(Failure<O> failure, StepContext ctx) {
        errorInvocations.add(new ErrorInvocation<>(failure, ctx));
    }

    public synchronized List<StartInvocation<I>> onStartInvocations() {
        return Collections.unmodifiableList(new ArrayList<>(startInvocations));
    }

    public synchronized List<FinishInvocation<O>> onFinishInvocations() {
        return Collections.unmodifiableList(new ArrayList<>(finishInvocations));
    }

    public synchronized List<ErrorInvocation<O>> onErrorInvocations() {
        return Collections.unmodifiableList(new ArrayList<>(errorInvocations));
    }
}
