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
import java.util.concurrent.Executors;
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

    /**
     * Runs {@code stepA} and {@code stepB} in parallel; each branch's output is automatically
     * written to state via its declared {@link io.flowpipe.api.StepDescriptor#outputKey()}.
     * The current pipeline value passes through unchanged to the next step.
     *
     * <p>Both branches <em>must</em> declare {@code withOutputKey(...)} on their
     * {@link io.flowpipe.api.StepDescriptor}; {@link Pipeline#build()} enforces this.
     */
    public PipelineBuilder<I, O> parallel2(Step<O, ?> stepA, Step<O, ?> stepB) {
        ensureUsable();
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        List<Step<O, ?>> branches = List.of(stepA, stepB);
        Function<List<Object>, O> noCombiner = null;
        nodes.add(new ParallelNode<>(branches, noCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, currentOutputType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    /**
     * Runs {@code stepA}, {@code stepB}, and {@code stepC} in parallel; each branch's output is
     * automatically written to state via its declared {@link io.flowpipe.api.StepDescriptor#outputKey()}.
     * The current pipeline value passes through unchanged to the next step.
     *
     * <p>All branches <em>must</em> declare {@code withOutputKey(...)}; {@link Pipeline#build()} enforces this.
     */
    public PipelineBuilder<I, O> parallel3(Step<O, ?> stepA, Step<O, ?> stepB, Step<O, ?> stepC) {
        ensureUsable();
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        Objects.requireNonNull(stepC, "stepC");
        List<Step<O, ?>> branches = List.of(stepA, stepB, stepC);
        Function<List<Object>, O> noCombiner = null;
        nodes.add(new ParallelNode<>(branches, noCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, currentOutputType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    /**
     * Runs all four steps in parallel; each branch's output is automatically written to state via
     * its declared {@link io.flowpipe.api.StepDescriptor#outputKey()}.
     * The current pipeline value passes through unchanged to the next step.
     *
     * <p>All branches <em>must</em> declare {@code withOutputKey(...)}; {@link Pipeline#build()} enforces this.
     */
    public PipelineBuilder<I, O> parallel4(
            Step<O, ?> stepA, Step<O, ?> stepB, Step<O, ?> stepC, Step<O, ?> stepD) {
        ensureUsable();
        Objects.requireNonNull(stepA, "stepA");
        Objects.requireNonNull(stepB, "stepB");
        Objects.requireNonNull(stepC, "stepC");
        Objects.requireNonNull(stepD, "stepD");
        List<Step<O, ?>> branches = List.of(stepA, stepB, stepC, stepD);
        Function<List<Object>, O> noCombiner = null;
        nodes.add(new ParallelNode<>(branches, noCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, currentOutputType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
    }

    /**
     * Runs all steps in {@code steps} in parallel; each branch's output is automatically written
     * to state via its declared {@link io.flowpipe.api.StepDescriptor#outputKey()}.
     * The current pipeline value passes through unchanged to the next step.
     *
     * <p>All branches <em>must</em> declare {@code withOutputKey(...)}; {@link Pipeline#build()} enforces this.
     * At least 2 steps are required.
     */
    public PipelineBuilder<I, O> parallelN(List<Step<O, ?>> steps) {
        ensureUsable();
        Objects.requireNonNull(steps, "steps");
        List<Step<O, ?>> branches = List.copyOf(steps);  // throws NPE if any element is null
        Function<List<Object>, O> noCombiner = null;
        nodes.add(new ParallelNode<>(branches, noCombiner, null));
        consumed = true;
        return new PipelineBuilder<>(inputType, currentOutputType, nodes, metricsRecorder, spanRecorder, executor, castLifecycle(lifecycle), deadlineMs);
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

    /**
     * Single-armed branch: if the predicate is true, routes to {@code ifTrue}; otherwise the
     * current pipeline value passes through unchanged (the false arm is a transparent identity).
     *
     * <p>Because the false arm is a pass-through, the ifTrue pipeline must accept and return the
     * same type — {@code Pipeline<O, O>}. The output type of the builder is therefore unchanged.
     *
     * <p>The pass-through arm appears in the {@link io.flowpipe.api.ExecutionTrace} as a skipped
     * step when the true arm is taken, and as a normal step (with no-op semantics) when the
     * predicate is false.
     */
    public PipelineBuilder<I, O> branch(
            String branchId,
            BiPredicate<O, StepContext> predicate,
            Pipeline<O, O> ifTrue) {
        Objects.requireNonNull(branchId, "branchId");
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(ifTrue, "ifTrue");
        Step<O, O> passThrough = Step.builder(
            branchId + ".pass-through", currentOutputType, currentOutputType).execute((v, ctx) -> v).build();
        Pipeline<O, O> passThroughPipeline = PipelineBuilder.start(currentOutputType)
            .then(passThrough)
            .build();
        return branch(branchId, predicate, ifTrue, passThroughPipeline);
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <E, R> PipelineBuilder<I, List<R>> foreach(Step<E, R> step) {
        ensureUsable();
        Objects.requireNonNull(step, "step");
        if (!currentOutputType.equals(List.class)) {
            throw new PipelineBuildException(
                "foreach requires the current pipeline output to be List, but was "
                    + currentOutputType.getName());
        }
        nodes.add(new ForeachNode<>(step));
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
        ExecutorService resolved = (executor != null) ? executor : DefaultExecutorHolder.INSTANCE;
        var circuitBreakers = buildCircuitBreakers(nodes);
        return new Pipeline<>(inputType, currentOutputType, List.copyOf(nodes), metricsRecorder,
            spanRecorder, resolved, lifecycle, circuitBreakers, deadlineMs);
    }

    // Lazy holder so the executor is created only when a pipeline that omits .withExecutor(...)
    // is actually built. Virtual threads (Java 21) give parallel composition unlimited
    // concurrency without pinning platform threads, and unlike ForkJoinPool.commonPool() the
    // pool is FlowPipe-private — it cannot starve unrelated CommonPool workloads on the JVM.
    private static final class DefaultExecutorHolder {
        static final ExecutorService INSTANCE = Executors.newVirtualThreadPerTaskExecutor();
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
            } else if (node instanceof ForeachNode<?, ?> fn) {
                var cbp = fn.step().describe().circuitBreakerPolicy();
                if (cbp != null) {
                    map.put(fn.step().describe().id(), FailsafePolicies.toFailsafe(cbp));
                }
            } else if (node instanceof ParallelNode<?, ?> pn) {
                for (Step<?, ?> branch : pn.branches()) {
                    var cbp = branch.describe().circuitBreakerPolicy();
                    if (cbp != null) {
                        map.put(branch.describe().id(), FailsafePolicies.toFailsafe(cbp));
                    }
                }
            }
            // Branch arms have their own Pipeline instances built via build(), which construct
            // their own circuit-breaker registry at that time.
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
                if (pn.combiner() == null) {
                    validateCombinerFreeOutputKeys(pn);
                }
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

    private static void validateCombinerFreeOutputKeys(ParallelNode<?, ?> pn) {
        var missing = new java.util.ArrayList<String>();
        for (Step<?, ?> branch : pn.branches()) {
            if (branch.describe().outputKey() == null) {
                missing.add(branch.describe().id());
            }
        }
        if (!missing.isEmpty()) {
            throw new PipelineBuildException(
                "Combiner-free parallel requires all branches to declare withOutputKey(...), "
                    + "but the following branches are missing it: " + missing);
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
