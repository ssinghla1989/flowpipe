package io.flowpipe.engine;

import io.flowpipe.api.Step;

import java.util.List;
import java.util.function.Function;

/**
 * @param declaredKeys non-null only for parallelN blocks; used at build() to verify map keys
 *                     match their step's descriptor id. Null for typed parallel2–4 blocks.
 */
record ParallelNode<I, O>(
    List<Step<I, ?>> branches,
    Function<List<Object>, O> combiner,
    List<String> declaredKeys
) implements EngineNode<I, O> {}
