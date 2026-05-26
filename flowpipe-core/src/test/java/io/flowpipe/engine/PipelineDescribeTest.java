package io.flowpipe.engine;

import io.flowpipe.api.NodeDescriptor;
import io.flowpipe.api.PipelineDescriptor;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.TimeoutPolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineDescribeTest {

    private static Step<String, Integer> parse(String id) {
        return Step.of(id, String.class, Integer.class, (s, ctx) -> Integer.parseInt(s));
    }

    private static Step<String, String> upper(String id) {
        return Step.of(id, String.class, String.class, (s, ctx) -> s.toUpperCase());
    }

    @Test
    void describe_exposes_pipeline_io_types() {
        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .then(parse("parse"))
            .build();

        PipelineDescriptor desc = pipeline.describe();

        assertThat(desc.inputType()).isEqualTo(String.class);
        assertThat(desc.outputType()).isEqualTo(Integer.class);
    }

    @Test
    void describe_lists_sequential_steps_in_order() {
        Step<Integer, String> render = Step.of("render", Integer.class, String.class,
            (i, ctx) -> Integer.toString(i));

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .then(parse("parse"))
            .then(render)
            .build();

        List<NodeDescriptor> nodes = pipeline.describe().nodes();
        assertThat(nodes).hasSize(2);

        assertThat(nodes.get(0)).isInstanceOfSatisfying(NodeDescriptor.Step.class,
            n -> assertThat(n.step().id()).isEqualTo("parse"));
        assertThat(nodes.get(1)).isInstanceOfSatisfying(NodeDescriptor.Step.class,
            n -> assertThat(n.step().id()).isEqualTo("render"));
    }

    @Test
    void describe_exposes_step_policies() {
        StepDescriptor<String, String> desc = StepDescriptor.builder("retrying", String.class, String.class)
            .withRetry(RetryPolicy.fixed(3, 10))
            .withTimeout(TimeoutPolicy.ofMillis(500))
            .build();
        Step<String, String> step = new Step<>() {
            @Override public StepDescriptor<String, String> describe() { return desc; }
            @Override public String execute(String input, io.flowpipe.api.StepContext ctx) { return input; }
        };

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class).then(step).build();
        NodeDescriptor.Step node = (NodeDescriptor.Step) pipeline.describe().nodes().get(0);

        assertThat(node.step().retryPolicy().maxAttempts()).isEqualTo(3);
        assertThat(node.step().timeoutPolicy().timeoutMs()).isEqualTo(500);
    }

    @Test
    void describe_parallel_lists_branch_steps() {
        Step<String, Integer> lenA = Step.of("len-a", String.class, Integer.class, (s, ctx) -> s.length());
        Step<String, Integer> lenB = Step.of("len-b", String.class, Integer.class, (s, ctx) -> s.length() * 2);

        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .parallel2(Integer::sum, lenA, lenB)
            .build();

        NodeDescriptor.Parallel parallel = (NodeDescriptor.Parallel) pipeline.describe().nodes().get(0);

        assertThat(parallel.branches()).hasSize(2);
        assertThat(parallel.branches().stream().map(StepDescriptor::id))
            .containsExactly("len-a", "len-b");
        assertThat(parallel.declaredKeys()).isNull();
    }

    @Test
    void describe_parallelN_exposes_declared_keys() {
        Step<String, Integer> a = Step.of("a", String.class, Integer.class, (s, ctx) -> 1);
        Step<String, Integer> b = Step.of("b", String.class, Integer.class, (s, ctx) -> 2);

        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .parallelN(Integer.class, Map.of("a", a, "b", b),
                results -> (Integer) results.get("a") + (Integer) results.get("b"))
            .build();

        NodeDescriptor.Parallel parallel = (NodeDescriptor.Parallel) pipeline.describe().nodes().get(0);

        assertThat(parallel.declaredKeys()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void describe_branch_recurses_into_arms() {
        Step<String, String> truthy = upper("truthy");
        Step<String, String> falsy = Step.of("falsy", String.class, String.class, (s, ctx) -> s.toLowerCase());

        Pipeline<String, String> ifTrue = PipelineBuilder.start(String.class).then(truthy).build();
        Pipeline<String, String> ifFalse = PipelineBuilder.start(String.class).then(falsy).build();

        Pipeline<String, String> pipeline = PipelineBuilder.start(String.class)
            .branch("decide", (input, ctx) -> input.length() > 3, ifTrue, ifFalse)
            .build();

        NodeDescriptor.Branch branch = (NodeDescriptor.Branch) pipeline.describe().nodes().get(0);

        assertThat(branch.branchId()).isEqualTo("decide");
        assertThat(branch.ifTrue().nodes()).hasSize(1);
        assertThat(branch.ifFalse().nodes()).hasSize(1);
        assertThat(((NodeDescriptor.Step) branch.ifTrue().nodes().get(0)).step().id()).isEqualTo("truthy");
        assertThat(((NodeDescriptor.Step) branch.ifFalse().nodes().get(0)).step().id()).isEqualTo("falsy");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void describe_foreach_exposes_inner_step_and_concurrency() {
        Pipeline<List<String>, List<String>> pipeline = (Pipeline<List<String>, List<String>>) (Pipeline)
            PipelineBuilder.start((Class<List<String>>) (Class<?>) List.class)
                .foreach(upper("up"), 4)
                .build();

        NodeDescriptor.Foreach foreach = (NodeDescriptor.Foreach) pipeline.describe().nodes().get(0);

        assertThat(foreach.step().id()).isEqualTo("up");
        assertThat(foreach.concurrency()).isEqualTo(4);
    }

    @Test
    void describe_returns_immutable_node_list() {
        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .then(parse("parse"))
            .build();

        List<NodeDescriptor> nodes = pipeline.describe().nodes();

        assertThatThrownBy(() -> nodes.add(new NodeDescriptor.Step(parse("other").describe())))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void describe_is_stable_across_calls() {
        Pipeline<String, Integer> pipeline = PipelineBuilder.start(String.class)
            .then(parse("parse"))
            .build();

        PipelineDescriptor first = pipeline.describe();
        PipelineDescriptor second = pipeline.describe();

        assertThat(first.nodes()).hasSize(second.nodes().size());
        assertThat(((NodeDescriptor.Step) first.nodes().get(0)).step().id())
            .isEqualTo(((NodeDescriptor.Step) second.nodes().get(0)).step().id());
    }
}
