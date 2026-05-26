package io.flowpipe.api;

import java.util.List;
import java.util.Objects;

public record PipelineDescriptor(
    Class<?> inputType,
    Class<?> outputType,
    List<NodeDescriptor> nodes
) {
    public PipelineDescriptor {
        Objects.requireNonNull(inputType, "inputType");
        Objects.requireNonNull(outputType, "outputType");
        Objects.requireNonNull(nodes, "nodes");
        nodes = List.copyOf(nodes);
    }
}
