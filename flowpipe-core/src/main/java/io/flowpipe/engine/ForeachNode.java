package io.flowpipe.engine;

import io.flowpipe.api.Step;

record ForeachNode<E, R>(Step<E, R> step) implements EngineNode<java.util.List<E>, java.util.List<R>> {}
