package io.flowpipe.api;

import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;

public interface StepContext {

    State state();

    RequestContext context();
}
