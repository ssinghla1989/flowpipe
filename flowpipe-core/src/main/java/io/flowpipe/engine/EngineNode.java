package io.flowpipe.engine;

sealed interface EngineNode<I, O> permits StepNode, ParallelNode, BranchNode, ForeachNode {}
