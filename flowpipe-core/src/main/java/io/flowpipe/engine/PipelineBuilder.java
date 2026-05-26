package io.flowpipe.engine;

import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.TriFunction;
import io.flowpipe.api.QuadFunction;
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.NoOpMetricsRecorder;
import io.flowpipe.observability.NoOpSpanRecorder;
import io.flowpipe.observability.SpanRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

public final class PipelineBuilder<I, O> {

    private static final PipelineLifecycle<Object, Object> NOOP_LIFECYCLE = new PipelineLifecycle<>() {};

    private final Class<I> inputType;
    private final Class<O> currentOutputType;
    private final List<EngineNode<?, ?>> nodes;
    private MetricsRecorder metricsRecorder;
    private SpanRecorder spanRecorder;
    private ExecutorService executor;
    private PipelineLifecycle<I, O> lifecycle;
    private long deadlineMs;
    private boolean consumed;

    @SuppressWarnings("unchecked")
    private PipelineBuilder(Class<I> inputType,
                            Class<O> currentOutputType,
                            List<EngineNode<?, ?>> nodes,
                            MetricsRecorder metricsRecorder,
                            SpanRecorder spanRecorder,
                            ExecutorService executor,
                            PipelineLifecycle<I, O> lifecycle,
                            long deadlineMs) {
        this.inputType = inputType;
        this.currentOutputType = currentOutputType;
        this.nodes = nodes;
        this.metricsRecorder = metricsRecorder;
        this.spanRecorder = spanRecorder;
        this.executor = executor;
        this.lifecycle = lifecycle;
        this.deadlineMs = deadlineMs;
    }

    @SuppressWarnings("unchecked")
    public static <I> PipelineBuilder<I, I> start(Class<I> inputType) {
        Objects.requireNonNull(inputType, "inputType");
        return new PipelineBuilder<>(inputType, inputType, new ArrayList<>(),
            NoOpMetricsRecorder.instance(), NoOpSpanRecorder.instance(), null,
            (PipelineLifecycle<I, I>) (PipelineLifecycle<?, ?>) NOOP_LIFECYCLE, 0L);
    }

    public <X> PipelineBuilder<I, X> then(Step<O, X> next) {
        ensureUsable();
        Objects.requireNonNull(next, "next");
        Class<?> declaredInput = next.describe().inputType();
        if (!declaredInput.equals(currentOutputType)) {
            throw new PipelineBuildException(
                "Step '" + next.describe().id() + "' declares inputType "
                    + declaredInput.getName()
                    + " but the previous output was "
                    + currentOutputType.getName());
        }
        nodes.add(new StepNode<>(next));
        consumed = true;
        return new PipelineBuilder<>(inputType, next.describe().outputType(), nodes, metricsRecorder, spanRecorder, executor,
            castLifecycle(lifecycle), deadlineMs);
    }

    public <A, B, R> PipelineBuilder<I, R> parallel2(
            Class<R> resultType,
            BiFunction<A, B, R> combiner,
            Step<O, A> stepA,
            Step<O, B> stepB) {
        ensureUsable();
        Objects.requireNonNull(resultType, "resultType");
        Objects.requireNonNull(combiner, "combiner");
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        List<Step<O, ?>> branches = List.of(stepA, stepB);
        Function<List<Object>, R> adaptedCombiner = results -> {
            @SuppressWarnings("unchecked")
            A a = (A) results.get(0);
            @SuppressWarnings("unchecked")
            B b = (B) results.get(1);
            return combiner.apply(a, b);
        };
        nodes.add(new ParallelNode<>(branches, adaptedCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, resultType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    public <A, B, C, R> PipelineBuilder<I, R> parallel3(
            Class<R> resultType,
            TriFunction<A, B, C, R> combiner,
            Step<O, A> stepA,
            Step<O, B> stepB,
            Step<O, C> stepC) {
        ensureUsable();
        Objects.requireNonNull(resultType, "resultType");
        Objects.requireNonNull(combiner, "combiner");
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        Objects.requireNonNull(stepC, "stepC");
        List<Step<O, ?>> branches = List.of(stepA, stepB, stepC);
        Function<List<Object>, R> adaptedCombiner = results -> {
            @SuppressWarnings("unchecked")
            A a = (A) results.get(0);
            @SuppressWarnings("unchecked")
            B b = (B) results.get(1);
            @SuppressWarnings("unchecked")
            C c = (C) results.get(2);
            return combiner.apply(a, b, c);
        };
        nodes.add(new ParallelNode<>(branches, adaptedCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, resultType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    public <A, B, C, D, R> PipelineBuilder<I, R> parallel4(
            Class<R> resultType,
            QuadFunction<A, B, C, D, R> combiner,
            Step<O, A> stepA,
            Step<O, B> stepB,
            Step<O, C> stepC,
            Step<O, D> stepD) {
        ensureUsable();
        Objects.requireNonNull(resultType, "resultType");
        Objects.requireNonNull(combiner, "combiner");
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        Objects.requireNonNull(stepC, "stepC");
        Objects.requireNonNull(stepD, "stepD");
        List<Step<O, ?>> branches = List.of(stepA, stepB, stepC, stepD);
        Function<List<Object>, R> adaptedCombiner = results -> {
            @SuppressWarnings("unchecked")
            A a = (A) results.get(0);
            @SuppressWarnings("unchecked")
            B b = (B) results.get(1);
            @SuppressWarnings("unchecked")
            C c = (C) results.get(2);
            @SuppressWarnings("unchecked")
            D d = (D) results.get(3);
            return combiner.apply(a, b, c, d);
        };
        nodes.add(new ParallelNode<>(branches, adaptedCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, resultType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    public <R> PipelineBuilder<I, R> parallelN(
            Class<R> resultType,
            java.util.Map<String, Step<O, ?>> steps,
            Function<java.util.Map<String, Object>, R> combiner) {
        ensureUsable();
        Objects.requireNonNull(resultType, "resultType");
        Objects.requireNonNull(steps, "steps");
        Objects.requireNonNull(combiner, "combiner");
        List<Step<O, ?>> branches = new ArrayList<>(steps.values());
        List<String> keyOrder = new ArrayList<>(steps.keySet());
        Function<List<Object>, R> adaptedCombiner = results -> {
            var map = new java.util.LinkedHashMap<String, Object>();
            for (int i = 0; i < keyOrder.size(); i++) {
                map.put(keyOrder.get(i), results.get(i));
            }
            return combiner.apply(map);
        };
        nodes.add(new ParallelNode<>(branches, adaptedCombiner, List.copyOf(keyOrder)));
        consumed = true;
        return new PipelineBuilder<>(inputType, resultType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <R> PipelineBuilder<I, R> branch(
            String branchId,
            BiPredicate<O, StepContext> predicate,
            Pipeline<O, R> ifTrue,
            Pipeline<O, R> ifFalse) {
        ensureUsable();
        Objects.requireNonNull(branchId, "branchId");
        if (branchId.isBlank()) {
            throw new IllegalArgumentException("branchId must not be blank");
        }
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(ifTrue, "ifTrue");
        Objects.requireNonNull(ifFalse, "ifFalse");
        if (!ifTrue.inputType().equals(currentOutputType)) {
            throw new PipelineBuildException(
                "Branch '" + branchId + "' ifTrue arm declares inputType "
                    + ifTrue.inputType().getName()
                    + " but the current pipeline output type is "
                    + currentOutputType.getName());
        }
        if (!ifFalse.inputType().equals(currentOutputType)) {
            throw new PipelineBuildException(
                "Branch '" + branchId + "' ifFalse arm declares inputType "
                    + ifFalse.inputType().getName()
                    + " but the current pipeline output type is "
                    + currentOutputType.getName());
        }
        if (!ifTrue.outputType().equals(ifFalse.outputType())) {
            throw new PipelineBuildException(
                "Branch '" + branchId + "' arms must produce the same output type, but ifTrue produces "
                    + ifTrue.outputType().getName()
                    + " and ifFalse produces "
                    + ifFalse.outputType().getName());
        }
        BiPredicate<Object, StepContext> rawPredicate = (BiPredicate) predicate;
        nodes.add(new BranchNode<>(branchId, rawPredicate, ifTrue, ifFalse));
        consumed = true;
        return new PipelineBuilder<>(inputType, ifTrue.outputType(), nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    public <E, R> PipelineBuilder<I, List<R>> foreach(Step<E, R> step) {
        return foreach(step, 1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E, R> PipelineBuilder<I, List<R>> foreach(Step<E, R> step, int concurrency) {
        ensureUsable();
        Objects.requireNonNull(step, "step");
        if (!currentOutputType.equals(List.class)) {
            throw new PipelineBuildException(
                "foreach requires the current pipeline output to be List, but was "
                    + currentOutputType.getName());
        }
        if (concurrency < 1) {
            throw new PipelineBuildException(
                "foreach concurrency must be >= 1, but was " + concurrency);
        }
        nodes.add(new ForeachNode<>(step, concurrency));
        consumed = true;
        return new PipelineBuilder<>(inputType, (Class<List<R>>) (Class) List.class, nodes, metricsRecorder, spanRecorder, executor,
            castLifecycle(lifecycle), deadlineMs);
    }

    public PipelineBuilder<I, O> withMetrics(MetricsRecorder recorder) {
        ensureUsable();
        Objects.requireNonNull(recorder, "recorder");
        this.metricsRecorder = recorder;
        return this;
    }

    public PipelineBuilder<I, O> withTracing(SpanRecorder recorder) {
        ensureUsable();
        Objects.requireNonNull(recorder, "recorder");
        this.spanRecorder = recorder;
        return this;
    }

    public PipelineBuilder<I, O> withExecutor(ExecutorService executorService) {
        ensureUsable();
        Objects.requireNonNull(executorService, "executorService");
        this.executor = executorService;
        return this;
    }

    public PipelineBuilder<I, O> withLifecycle(PipelineLifecycle<I, O> lc) {
        ensureUsable();
        Objects.requireNonNull(lc, "lifecycle");
        this.lifecycle = lc;
        return this;
    }

    public PipelineBuilder<I, O> withDeadline(long ms) {
        ensureUsable();
        if (ms < 1) {
            throw new IllegalArgumentException("deadline ms must be >= 1, got: " + ms);
        }
        this.deadlineMs = ms;
        return this;
    }

    public PipelineBuilder<I, O> withDeadline(long duration, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        return withDeadline(unit.toMillis(duration));
    }

    public Pipeline<I, O> build() {
        ensureUsable();
        consumed = true;
        validate();
        ExecutorService resolved = (executor != null) ? executor : ForkJoinPool.commonPool();
        var circuitBreakers = buildCircuitBreakers(nodes);
        return new Pipeline<>(inputType, currentOutputType, List.copyOf(nodes), metricsRecorder,
            spanRecorder, resolved, lifecycle, circuitBreakers, deadlineMs);
    }

    private static java.util.Map<String, dev.failsafe.CircuitBreaker<Object>> buildCircuitBreakers(
            List<EngineNode<?, ?>> nodes) {
        var map = new java.util.HashMap<String, dev.failsafe.CircuitBreaker<Object>>();
        for (EngineNode<?, ?> node : nodes) {
            if (node instanceof StepNode<?, ?> sn) {
                var cbp = sn.step().describe().circuitBreakerPolicy();
                if (cbp != null) {
                    map.put(sn.step().describe().id(), FailsafePolicies.toFailsafe(cbp));
                }
            }
            // Branch arms and foreach steps are not pre-built here; they inherit from
            // the arm Pipeline instances which build their own registry at build() time.
        }
        return java.util.Collections.unmodifiableMap(map);
    }

    @SuppressWarnings("unchecked")
    private static <I, X> PipelineLifecycle<I, X> castLifecycle(PipelineLifecycle<I, ?> lc) {
        return (PipelineLifecycle<I, X>) lc;
    }

    private void ensureUsable() {
        if (consumed) {
            throw new IllegalStateException(
                "PipelineBuilder has already been consumed; obtain a new builder via PipelineBuilder.start(...)");
        }
    }

    private void validate() {
        if (nodes.isEmpty()) {
            throw new PipelineBuildException(
                "Cannot build an empty pipeline: at least one .then(step) call is required");
        }

        var seenIds = new java.util.HashSet<String>();
        var duplicates = new java.util.LinkedHashSet<String>();
        for (EngineNode<?, ?> node : nodes) {
            collectIds(node, seenIds, duplicates);
        }
        if (!duplicates.isEmpty()) {
            throw new PipelineBuildException(
                "Duplicate step ids in pipeline: " + duplicates);
        }

        for (EngineNode<?, ?> node : nodes) {
            if (node instanceof ParallelNode<?, ?> pn) {
                if (pn.branches().size() < 2) {
                    throw new PipelineBuildException(
                        "A parallel block requires at least 2 branches, but found " + pn.branches().size());
                }
                validateParallelNKeys(pn);
            }
        }
    }

    private static void collectIds(EngineNode<?, ?> node,
                                   java.util.Set<String> seenIds,
                                   java.util.Set<String> duplicates) {
        if (node instanceof StepNode<?, ?> sn) {
            String id = sn.step().describe().id();
            if (!seenIds.add(id)) {
                duplicates.add(id);
            }
        } else if (node instanceof ParallelNode<?, ?> pn) {
            for (Step<?, ?> branch : pn.branches()) {
                String id = branch.describe().id();
                if (!seenIds.add(id)) {
                    duplicates.add(id);
                }
            }
        } else if (node instanceof BranchNode<?, ?> bn) {
            String id = bn.branchId();
            if (!seenIds.add(id)) {
                duplicates.add(id);
            }
        } else if (node instanceof ForeachNode<?, ?> fn) {
            String id = fn.step().describe().id();
            if (!seenIds.add(id)) {
                duplicates.add(id);
            }
        }
    }

    private static void validateParallelNKeys(ParallelNode<?, ?> pn) {
        List<String> keys = pn.declaredKeys();
        if (keys == null) {
            return;
        }
        List<? extends Step<?, ?>> branches = pn.branches();
        var mismatches = new java.util.LinkedHashSet<String>();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String stepId = branches.get(i).describe().id();
            if (!key.equals(stepId)) {
                mismatches.add("key '" + key + "' does not match step id '" + stepId + "'");
            }
        }
        if (!mismatches.isEmpty()) {
            throw new PipelineBuildException(
                "parallelN map keys do not match step descriptor ids: " + mismatches);
        }
    }
}
