package io.flowpipe.api;

import java.util.List;
import java.util.Objects;

public sealed interface NodeDescriptor {

    record Step(StepDescriptor<?, ?> step) implements NodeDescriptor {
        public Step {
            Objects.requireNonNull(step, "step");
        }
    }

    record Parallel(List<StepDescriptor<?, ?>> branches, List<String> declaredKeys)
        implements NodeDescriptor {
        public Parallel {
            Objects.requireNonNull(branches, "branches");
            branches = List.copyOf(branches);
            declaredKeys = declaredKeys == null ? null : List.copyOf(declaredKeys);
        }
    }

    record Branch(String branchId, PipelineDescriptor ifTrue, PipelineDescriptor ifFalse)
        implements NodeDescriptor {
        public Branch {
            Objects.requireNonNull(branchId, "branchId");
            Objects.requireNonNull(ifTrue, "ifTrue");
            Objects.requireNonNull(ifFalse, "ifFalse");
        }
    }

    record Foreach(StepDescriptor<?, ?> step, int concurrency) implements NodeDescriptor {
        public Foreach {
            Objects.requireNonNull(step, "step");
            if (concurrency < 1) {
                throw new IllegalArgumentException("concurrency must be >= 1");
            }
        }
    }
}
