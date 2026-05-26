package io.flowpipe.observability;

import io.flowpipe.state.RequestContext;

public final class NoOpSpanRecorder implements SpanRecorder {

    private static final NoOpSpanRecorder INSTANCE = new NoOpSpanRecorder();

    private NoOpSpanRecorder() {
    }

    public static NoOpSpanRecorder instance() {
        return INSTANCE;
    }

    @Override
    public Object startStep(String stepId, RequestContext context) {
        return null;
    }

    @Override
    public void finishStep(Object span, StepOutcome outcome, Throwable cause) {
    }
}
