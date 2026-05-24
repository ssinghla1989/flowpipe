package io.flowpipe.engine;

import io.flowpipe.api.Step;

record StepNode<I, O>(Step<I, O> step) implements EngineNode<I, O> {}
