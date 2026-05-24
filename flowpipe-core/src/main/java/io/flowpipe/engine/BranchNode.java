package io.flowpipe.engine;

import io.flowpipe.api.StepContext;

import java.util.function.BiPredicate;

record BranchNode<I, O>(
    String branchId,
    BiPredicate<Object, StepContext> predicate,
    Pipeline<?, ?> ifTrue,
    Pipeline<?, ?> ifFalse
) implements EngineNode<I, O> {}
