package io.flowpipe.engine;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.Policy;
import dev.failsafe.TimeoutExceededException;
import io.flowpipe.api.CircuitBreakerOpenException;
import io.flowpipe.api.ExecutionTrace;
import io.flowpipe.api.Failure;
import io.flowpipe.api.NodeDescriptor;
import io.flowpipe.api.PipelineDescriptor;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.StepTimeoutException;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.api.Success;
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class Pipeline<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private final Class<I> inputType;
    private final Class<O> outputType;
    private final List<EngineNode<?, ?>> nodes;
    private final MetricsRecorder defaultRecorder;
    private final ExecutorService executor;
    private final PipelineLifecycle<I, O> lifecycle;
    private final Map<String, dev.failsafe.CircuitBreaker<Object>> circuitBreakers;

    Pipeline(Class<I> inputType,
             Class<O> outputType,
             List<EngineNode<?, ?>> nodes,
             MetricsRecorder defaultRecorder,
             ExecutorService executor,
             PipelineLifecycle<I, O> lifecycle,
             Map<String, dev.failsafe.CircuitBreaker<Object>> circuitBreakers) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.nodes = nodes;
        this.defaultRecorder = defaultRecorder;
        this.executor = executor;
        this.lifecycle = lifecycle;
        this.circuitBreakers = circuitBreakers;
    }

    public Class<I> inputType() {
        return inputType;
    }

    public Class<O> outputType() {
        return outputType;
    }

    public PipelineDescriptor describe() {
        List<NodeDescriptor> descriptors = new ArrayList<>(nodes.size());
        for (EngineNode<?, ?> node : nodes) {
            descriptors.add(toDescriptor(node));
        }
        return new PipelineDescriptor(inputType, outputType, descriptors);
    }

    private static NodeDescriptor toDescriptor(EngineNode<?, ?> node) {
        if (node instanceof StepNode<?, ?> sn) {
            return new NodeDescriptor.Step(sn.step().describe());
        } else if (node instanceof ParallelNode<?, ?> pn) {
            List<StepDescriptor<?, ?>> branches = new ArrayList<>(pn.branches().size());
            for (Step<?, ?> branch : pn.branches()) {
                branches.add(branch.describe());
            }
            return new NodeDescriptor.Parallel(branches, pn.declaredKeys());
        } else if (node instanceof BranchNode<?, ?> bn) {
            return new NodeDescriptor.Branch(bn.branchId(), bn.ifTrue().describe(), bn.ifFalse().describe());
        } else if (node instanceof ForeachNode<?, ?> fn) {
            return new NodeDescriptor.Foreach(fn.step().describe(), fn.concurrency());
        }
        throw new IllegalStateException("Unknown node type: " + node.getClass());
    }

    public Result<O> execute(I input) {
        return execute(input, RequestContext.empty(), defaultRecorder);
    }

    public Result<O> execute(I input, RequestContext context) {
        return execute(input, context, defaultRecorder);
    }

    public Result<O> execute(I input, RequestContext context, MetricsRecorder recorder) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(recorder, "recorder");
        State state = new State();
        DefaultStepContext ctx = new DefaultStepContext(state, context);
        ExecutionTrace.Builder traceBuilder = ExecutionTrace.builder();

        try {
            lifecycle.onStart(input, ctx);
        } catch (Throwable t) {
            return new Failure<>(t, "pipeline.onStart", traceBuilder.build());
        }

        @SuppressWarnings("unchecked")
        Result<O> result = (Result<O>) executeShared(input, ctx, context, recorder, traceBuilder);

        safeLifecycleCall("onFinish", () -> lifecycle.onFinish(result, ctx));
        if (result instanceof Failure<O> failure) {
            safeLifecycleCall("onError", () -> lifecycle.onError(failure, ctx));
        }

        return result;
    }

    private static void safeLifecycleCall(String op, RunnableEx call) {
        try {
            call.run();
        } catch (Throwable t) {
            LOG.atWarn()
                .setMessage("lifecycle.hook_failed")
                .addKeyValue("lifecycle.op", op)
                .addKeyValue("error.class", t.getClass().getName())
                .addKeyValue("error.message", t.getMessage() == null ? "" : t.getMessage())
                .log();
        }
    }

    @FunctionalInterface
    private interface RunnableEx {
        void run() throws Exception;
    }

    // Package-private: called by executeBranch on arm pipelines to share State/context/recorder/traceBuilder.
    Result<?> executeShared(Object input, DefaultStepContext ctx, RequestContext context,
                            MetricsRecorder recorder, ExecutionTrace.Builder traceBuilder) {
        Object current = input;
        for (EngineNode<?, ?> node : nodes) {
            if (node instanceof StepNode<?, ?> sn) {
                String stepId = sn.step().describe().id();
                ItemResult result = executeItemWithRetry(sn.step(), current, stepId, ctx, context, recorder);
                traceBuilder.append(result.trace());
                if (result instanceof ItemFailure f) {
                    return new Failure<>(f.cause(), f.stepId(), traceBuilder.build());
                }
                current = ((ItemSuccess) result).value();
            } else if (node instanceof ParallelNode<?, ?> pn) {
                ParallelOutcome parallelResult = executeParallel(pn, current, ctx, context, recorder, traceBuilder);
                if (parallelResult instanceof ParallelFailure pf) {
                    return pf.failure();
                }
                current = ((ParallelSuccess) parallelResult).value();
            } else if (node instanceof BranchNode<?, ?> bn) {
                BranchOutcome branchResult = executeBranch(bn, current, ctx, context, recorder, traceBuilder);
                if (branchResult instanceof BranchFailure bf) {
                    return bf.failure();
                }
                current = ((BranchSuccess) branchResult).value();
            } else if (node instanceof ForeachNode<?, ?> fn) {
                ForeachOutcome foreachResult = executeForeach(fn, current, ctx, context, recorder, traceBuilder);
                if (foreachResult instanceof ForeachFailure ff) {
                    return ff.failure();
                }
                current = ((ForeachSuccess) foreachResult).value();
            }
        }
        return new Success<>(current, traceBuilder.build());
    }

    // Per-item execution result — carries the TraceEntry so callers append to the shared
    // traceBuilder on the main thread (avoiding ArrayList concurrency issues).
    private sealed interface ItemResult permits ItemSuccess, ItemFailure {
        TraceEntry trace();
    }
    private record ItemSuccess(Object value, TraceEntry trace) implements ItemResult {}
    private record ItemFailure(Throwable cause, String stepId, TraceEntry trace) implements ItemResult {}

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ItemResult executeItemWithRetry(Step step, Object input, String itemLabel,
                                            DefaultStepContext stepCtx, RequestContext context,
                                            MetricsRecorder recorder) {
        RetryPolicy fpRetry = step.describe().retryPolicy();
        io.flowpipe.api.TimeoutPolicy fpTimeout = step.describe().timeoutPolicy();
        int maxAttempts = fpRetry.maxAttempts();
        long timeoutMs = fpTimeout.timeoutMs();

        // Build Failsafe policy chain in outer-to-inner order:
        // [circuitBreaker (optional)] → [retryPolicy (if maxAttempts > 1)] → [timeout (if set)]
        List<Policy<Object>> policies = new ArrayList<>();

        dev.failsafe.CircuitBreaker<Object> fsCb = circuitBreakers.get(step.describe().id());
        if (fsCb != null) policies.add(fsCb);

        if (maxAttempts > 1) {
            var retryBuilder = FailsafePolicies.toFailsafe(fpRetry);
            retryBuilder.onFailedAttempt(event -> {
                int attempt = event.getAttemptCount();  // already 1-indexed in event listeners
                long durationNanos = event.getElapsedAttemptTime().toNanos();
                Throwable t = event.getLastException();
                if (t instanceof TimeoutExceededException) {
                    t = new StepTimeoutException(itemLabel, timeoutMs);
                } else if (t instanceof FailsafeException fe && fe.getCause() != null) {
                    t = fe.getCause();
                }
                emitError(itemLabel, attempt, durationNanos, context, t);
            });
            retryBuilder.onRetryScheduled(event -> {
                int attempt = event.getAttemptCount();  // already 1-indexed in event listeners
                long delayMs = event.getDelay().toMillis();
                emitRetry(itemLabel, attempt, maxAttempts, delayMs, context);
                safeRecord(itemLabel, "recordRetryAttempt",
                    () -> recorder.recordRetryAttempt(itemLabel, attempt));
            });
            policies.add(retryBuilder.build());
        }

        if (timeoutMs > 0) policies.add(FailsafePolicies.toFailsafe(fpTimeout));

        long totalStartNanos = System.nanoTime();
        AtomicInteger lastAttempt = new AtomicInteger(1);

        try {
            Object output;
            if (policies.isEmpty()) {
                // Fast path: no resilience policies — call directly without Failsafe overhead
                emitStart(itemLabel, 1, context);
                output = invokeStep(step, input, stepCtx);
            } else {
                output = Failsafe.with(policies).get((dev.failsafe.ExecutionContext<Object> ctx) -> {
                    int attempt = ctx.getAttemptCount() + 1;  // Failsafe is 0-indexed
                    lastAttempt.set(attempt);
                    emitStart(itemLabel, attempt, context);
                    return invokeStep(step, input, stepCtx);
                });
            }

            int attempt = lastAttempt.get();
            long totalDurationNanos = System.nanoTime() - totalStartNanos;
            emitRecord(recorder, itemLabel, totalDurationNanos, attempt, StepOutcome.SUCCESS);
            emitFinish(itemLabel, attempt, totalDurationNanos, context);
            return new ItemSuccess(output, new TraceEntry(itemLabel, totalStartNanos, totalDurationNanos, attempt, false));

        } catch (dev.failsafe.CircuitBreakerOpenException cboe) {
            long durationNanos = System.nanoTime() - totalStartNanos;
            Instant retriableAfter = Instant.now().plus(cboe.getCircuitBreaker().getRemainingDelay());
            CircuitBreakerOpenException fpEx = new CircuitBreakerOpenException(itemLabel, retriableAfter);
            emitCircuitOpen(itemLabel, retriableAfter, context);
            emitRecord(recorder, itemLabel, durationNanos, 1, StepOutcome.FAILURE);
            return new ItemFailure(fpEx, itemLabel,
                new TraceEntry(itemLabel, totalStartNanos, durationNanos, 1, false));

        } catch (TimeoutExceededException tee) {
            // Thrown when no retry is configured, or when all retry attempts timed out.
            // When maxAttempts > 1, onFailedAttempt already emitted step.error per attempt.
            long durationNanos = System.nanoTime() - totalStartNanos;
            int attempt = lastAttempt.get();
            StepTimeoutException ste = new StepTimeoutException(itemLabel, timeoutMs);
            if (maxAttempts <= 1) emitError(itemLabel, attempt, durationNanos, context, ste);
            emitRecord(recorder, itemLabel, durationNanos, attempt, StepOutcome.FAILURE);
            return new ItemFailure(ste, itemLabel,
                new TraceEntry(itemLabel, totalStartNanos, durationNanos, attempt, false));

        } catch (Throwable t) {
            // Unwrap Failsafe wrapper if present so the original exception surfaces in Failure.cause().
            Throwable cause = (t instanceof FailsafeException fe && fe.getCause() != null)
                ? fe.getCause() : t;
            long durationNanos = System.nanoTime() - totalStartNanos;
            int attempt = lastAttempt.get();
            // When maxAttempts > 1, onFailedAttempt already emitted step.error for every attempt.
            if (maxAttempts <= 1) emitError(itemLabel, attempt, durationNanos, context, cause);
            emitRecord(recorder, itemLabel, durationNanos, attempt, StepOutcome.FAILURE);
            return new ItemFailure(cause, itemLabel,
                new TraceEntry(itemLabel, totalStartNanos, durationNanos, attempt, false));
        }
    }

    private sealed interface ParallelOutcome permits ParallelSuccess, ParallelFailure {}
    private record ParallelSuccess(Object value) implements ParallelOutcome {}
    private record ParallelFailure(Failure<Object> failure) implements ParallelOutcome {}

    private sealed interface BranchOutcome permits BranchSuccess, BranchFailure {}
    private record BranchSuccess(Object value) implements BranchOutcome {}
    private record BranchFailure(Failure<Object> failure) implements BranchOutcome {}

    private sealed interface ForeachOutcome permits ForeachSuccess, ForeachFailure {}
    private record ForeachSuccess(Object value) implements ForeachOutcome {}
    private record ForeachFailure(Failure<Object> failure) implements ForeachOutcome {}

    @SuppressWarnings({"unchecked", "rawtypes"})
    private BranchOutcome executeBranch(BranchNode bn, Object input, DefaultStepContext ctx,
                                        RequestContext context, MetricsRecorder recorder,
                                        ExecutionTrace.Builder traceBuilder) {
        String branchId = bn.branchId();
        long startedAtNanos = System.nanoTime();
        boolean selected;
        try {
            selected = bn.predicate().test(input, ctx);
        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startedAtNanos;
            traceBuilder.append(new TraceEntry(branchId, startedAtNanos, durationNanos, 1, false));
            return new BranchFailure(new Failure<>(t, branchId, traceBuilder.build()));
        }
        long durationNanos = System.nanoTime() - startedAtNanos;
        traceBuilder.append(new TraceEntry(branchId, startedAtNanos, durationNanos, 1, false));

        Pipeline takenArm = selected ? bn.ifTrue() : bn.ifFalse();
        Pipeline skippedArm = selected ? bn.ifFalse() : bn.ifTrue();

        Result<?> armResult = takenArm.executeShared(input, ctx, context, recorder, traceBuilder);
        if (armResult instanceof Failure<?> f) {
            return new BranchFailure((Failure<Object>) f);
        }
        Object value = ((Success<?>) armResult).value();

        emitSkipped(skippedArm.nodes, branchId, recorder, traceBuilder, context);
        return new BranchSuccess(value);
    }

    private static void emitSkipped(List<EngineNode<?, ?>> skippedNodes, String branchId,
                                    MetricsRecorder recorder, ExecutionTrace.Builder traceBuilder,
                                    RequestContext context) {
        for (EngineNode<?, ?> node : skippedNodes) {
            if (node instanceof StepNode<?, ?> sn) {
                String stepId = sn.step().describe().id();
                traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                emitSkip(stepId, branchId, context);
            } else if (node instanceof ParallelNode<?, ?> pn) {
                for (Step<?, ?> branch : pn.branches()) {
                    String stepId = branch.describe().id();
                    traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                    emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                    emitSkip(stepId, branchId, context);
                }
            } else if (node instanceof BranchNode<?, ?> bn) {
                traceBuilder.append(new TraceEntry(bn.branchId(), 0L, 0L, 0, true));
                emitSkip(bn.branchId(), branchId, context);
                emitSkipped(bn.ifTrue().nodes, branchId, recorder, traceBuilder, context);
                emitSkipped(bn.ifFalse().nodes, branchId, recorder, traceBuilder, context);
            } else if (node instanceof ForeachNode<?, ?> fn) {
                String stepId = fn.step().describe().id();
                traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                emitSkip(stepId, branchId, context);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ParallelOutcome executeParallel(ParallelNode pn, Object input, DefaultStepContext ctx,
                                            RequestContext context, MetricsRecorder recorder,
                                            ExecutionTrace.Builder traceBuilder) {
        List<Step<Object, ?>> branches = pn.branches();
        List<Future<BranchResult>> futures = new ArrayList<>(branches.size());

        for (Step<Object, ?> branch : branches) {
            String stepId = branch.describe().id();
            Callable<BranchResult> task = () -> {
                emitStart(stepId, 1, context);
                long startedAtNanos = System.nanoTime();
                try {
                    Object output = invokeStep(branch, input, ctx);
                    long durationNanos = System.nanoTime() - startedAtNanos;
                    TraceEntry entry = new TraceEntry(stepId, startedAtNanos, durationNanos, 1, false);
                    emitRecord(recorder, stepId, durationNanos, 1, StepOutcome.SUCCESS);
                    emitFinish(stepId, 1, durationNanos, context);
                    return new BranchResult(stepId, output, entry, null);
                } catch (Throwable t) {
                    long durationNanos = System.nanoTime() - startedAtNanos;
                    TraceEntry entry = new TraceEntry(stepId, startedAtNanos, durationNanos, 1, false);
                    emitRecord(recorder, stepId, durationNanos, 1, StepOutcome.FAILURE);
                    emitError(stepId, 1, durationNanos, context, t);
                    return new BranchResult(stepId, null, entry, t);
                }
            };

            try {
                futures.add(executor.submit(task));
            } catch (RejectedExecutionException e) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e, stepId, traceBuilder.build()));
            }
        }

        List<Object> outputs = new ArrayList<>(branches.size());

        for (Future<BranchResult> future : futures) {
            BranchResult br;
            try {
                br = future.get();
            } catch (ExecutionException e) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e.getCause(), "unknown", traceBuilder.build()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e, "interrupted", traceBuilder.build()));
            }

            traceBuilder.append(br.entry());
            if (br.failure() != null) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(br.failure(), br.stepId(), traceBuilder.build()));
            }
            outputs.add(br.output());
        }

        Object combined = pn.combiner().apply(outputs);
        return new ParallelSuccess(combined);
    }

    @SuppressWarnings("rawtypes")
    private ForeachOutcome executeForeach(ForeachNode fn, Object input, DefaultStepContext ctx,
                                          RequestContext context, MetricsRecorder recorder,
                                          ExecutionTrace.Builder traceBuilder) {
        List<?> items = (List<?>) input;
        String stepId = fn.step().describe().id();
        int concurrency = fn.concurrency();
        List<Object> outputs = new ArrayList<>(items.size());

        if (concurrency == 1) {
            for (int i = 0; i < items.size(); i++) {
                String itemLabel = stepId + "[" + i + "]";
                ItemResult result = executeItemWithRetry(fn.step(), items.get(i), itemLabel, ctx, context, recorder);
                traceBuilder.append(result.trace());
                if (result instanceof ItemFailure f) {
                    return new ForeachFailure(new Failure<>(f.cause(), f.stepId(), traceBuilder.build()));
                }
                outputs.add(((ItemSuccess) result).value());
            }
        } else {
            for (int windowStart = 0; windowStart < items.size(); windowStart += concurrency) {
                int windowEnd = Math.min(windowStart + concurrency, items.size());
                List<Future<ItemResult>> futures = new ArrayList<>(windowEnd - windowStart);

                for (int i = windowStart; i < windowEnd; i++) {
                    final int index = i;
                    final String itemLabel = stepId + "[" + index + "]";
                    try {
                        futures.add(executor.submit(
                            () -> executeItemWithRetry(fn.step(), items.get(index), itemLabel, ctx, context, recorder)));
                    } catch (RejectedExecutionException e) {
                        cancelAll(futures);
                        return new ForeachFailure(new Failure<>(e, stepId, traceBuilder.build()));
                    }
                }

                for (Future<ItemResult> future : futures) {
                    ItemResult result;
                    try {
                        result = future.get();
                    } catch (ExecutionException e) {
                        cancelAll(futures);
                        return new ForeachFailure(new Failure<>(e.getCause(), stepId, traceBuilder.build()));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        cancelAll(futures);
                        return new ForeachFailure(new Failure<>(e, stepId, traceBuilder.build()));
                    }

                    traceBuilder.append(result.trace());
                    if (result instanceof ItemFailure f) {
                        cancelAll(futures);
                        return new ForeachFailure(new Failure<>(f.cause(), f.stepId(), traceBuilder.build()));
                    }
                    outputs.add(((ItemSuccess) result).value());
                }
            }
        }

        return new ForeachSuccess(outputs);
    }

    private static void cancelAll(List<? extends Future<?>> futures) {
        for (Future<?> f : futures) {
            f.cancel(true);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object invokeStep(Step step, Object input, DefaultStepContext ctx) throws Exception {
        step.describe().inputValidator().validate(input);
        Object output = step.execute(input, ctx);
        step.describe().outputValidator().validate(output);
        return output;
    }

    private static void emitRetry(String stepId, int attempt, int maxAttempts, long delayMs,
                                  RequestContext context) {
        LoggingEventBuilder event = LOG.atWarn()
            .setMessage("step.retry")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.attempt", attempt)
            .addKeyValue("step.max_attempts", maxAttempts)
            .addKeyValue("step.delay_ms", delayMs);
        addContextFields(event, context);
        event.log();
    }

    private static void emitStart(String stepId, int attempts, RequestContext context) {
        LoggingEventBuilder event = LOG.atInfo()
            .setMessage("step.start")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.attempt", attempts);
        addContextFields(event, context);
        event.log();
    }

    private static void emitFinish(String stepId, int attempts, long durationNanos, RequestContext context) {
        LoggingEventBuilder event = LOG.atInfo()
            .setMessage("step.finish")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.attempt", attempts)
            .addKeyValue("step.duration_ms", TimeUnit.NANOSECONDS.toMillis(durationNanos))
            .addKeyValue("step.outcome", "success");
        addContextFields(event, context);
        event.log();
    }

    private static void emitError(String stepId, int attempts, long durationNanos,
                                  RequestContext context, Throwable cause) {
        LoggingEventBuilder event = LOG.atError()
            .setMessage("step.error")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.attempt", attempts)
            .addKeyValue("step.duration_ms", TimeUnit.NANOSECONDS.toMillis(durationNanos))
            .addKeyValue("step.outcome", "failure")
            .addKeyValue("step.error_class", cause.getClass().getName())
            .addKeyValue("step.error_message", cause.getMessage() == null ? "" : cause.getMessage());
        addContextFields(event, context);
        event.log();
    }

    private static void emitCircuitOpen(String stepId, Instant retriableAfter, RequestContext context) {
        LoggingEventBuilder event = LOG.atWarn()
            .setMessage("step.circuit_open")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.retriable_after", retriableAfter.toString());
        addContextFields(event, context);
        event.log();
    }

    private static void emitSkip(String stepId, String branchId, RequestContext context) {
        LoggingEventBuilder event = LOG.atDebug()
            .setMessage("step.skip")
            .addKeyValue("step.id", stepId)
            .addKeyValue("step.branch_id", branchId);
        addContextFields(event, context);
        event.log();
    }

    private static void addContextFields(LoggingEventBuilder event, RequestContext context) {
        context.forEach((key, value) -> event.addKeyValue(key.name(), value));
    }

    private static void emitRecord(MetricsRecorder recorder,
                                   String stepId,
                                   long durationNanos,
                                   int attempts,
                                   StepOutcome outcome) {
        safeRecord(stepId, "recordStepDuration",
            () -> recorder.recordStepDuration(stepId, durationNanos));
        safeRecord(stepId, "recordStepAttempts",
            () -> recorder.recordStepAttempts(stepId, attempts));
        safeRecord(stepId, "recordStepOutcome",
            () -> recorder.recordStepOutcome(stepId, outcome));
    }

    private static void safeRecord(String stepId, String op, Runnable call) {
        try {
            call.run();
        } catch (Throwable t) {
            LOG.atWarn()
                .setMessage("metrics.recorder_failed")
                .addKeyValue("step.id", stepId)
                .addKeyValue("metrics.op", op)
                .addKeyValue("error.class", t.getClass().getName())
                .addKeyValue("error.message", t.getMessage() == null ? "" : t.getMessage())
                .log();
        }
    }

    MetricsRecorder defaultRecorder() {
        return defaultRecorder;
    }

    private record BranchResult(String stepId, Object output, TraceEntry entry, Throwable failure) {}
}
