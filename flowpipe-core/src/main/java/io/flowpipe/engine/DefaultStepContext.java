package io.flowpipe.engine;

import io.flowpipe.api.StepContext;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;

import java.util.Objects;

final class DefaultStepContext implements StepContext {

    private final State state;
    private final RequestContext context;

    DefaultStepContext(State state, RequestContext context) {
        this.state = Objects.requireNonNull(state, "state");
        this.context = Objects.requireNonNull(context, "context");
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public RequestContext context() {
        return context;
    }
}
