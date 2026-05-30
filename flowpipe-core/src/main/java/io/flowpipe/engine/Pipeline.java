package io.flowpipe.engine;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.Policy;
import dev.failsafe.TimeoutExceededException;
import io.flowpipe.api.CircuitBreakerOpenException;
import io.flowpipe.api.ExecutionTrace;
import io.flowpipe.api.Failure;
import io.flowpipe.api.NodeDescriptor;
import io.flowpipe.api.PipelineDeadlineExceededException;
import io.flowpipe.api.PipelineDescriptor;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.api.StepTimeoutException;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.api.Success;
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.NoOpSpanRecorder;
import io.flowpipe.observability.SpanRecorder;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;
import io.flowpipe.state.StateKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public final class Pipeline<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private final Class<I> inputType;
    private final Class<O> outputType;
    private final List<EngineNode<?, ?>> nodes;
    private final MetricsRecorder defaultRecorder;
    private final SpanRecorder spanRecorder;
    private final ExecutorService executor;
    private final PipelineLifecycle<I, O> lifecycle;
    private final Map<String, dev.failsafe.CircuitBreaker<Object>> circuitBreakers;
    private final long deadlineMs;

    Pipeline(Class<I> inputType,
             Class<O> outputType,
             List<EngineNode<?, ?>> nodes,
             MetricsRecorder defaultRecorder,
             SpanRecorder spanRecorder,
             ExecutorService executor,
             PipelineLifecycle<I, O> lifecycle,
             Map<String, dev.failsafe.CircuitBreaker<Object>> circuitBreakers,
             long deadlineMs) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.nodes = nodes;
        this.defaultRecorder = defaultRecorder;
        this.spanRecorder = spanRecorder;
        this.executor = executor;
        this.lifecycle = lifecycle;
        this.circuitBreakers = circuitBreakers;
        this.deadlineMs = deadlineMs;
    }

    /**
     * Convenience alias for {@link PipelineBuilder#start(Class)} — discoverable entry point when
     * starting from the {@code Pipeline} class rather than {@code PipelineBuilder}.
     */
    public static <I> PipelineBuilder<I, I> builder(Class<I> inputType) {
        return PipelineBuilder.start(inputType);
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
            return new NodeDescriptor.Foreach(fn.step().describe());
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
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(recorder, "recorder");
        State state = new State();
        DefaultStepContext ctx = new DefaultStepContext(state, context);
        ExecutionTrace.Builder traceBuilder = ExecutionTrace.builder();

        try {
            lifecycle.onStart(input, ctx);
        } catch (Throwable t) {
            Failure<O> failure = new Failure<>(t, "pipeline.onStart", traceBuilder.build());
            safeLifecycleCall("onFinish", () -> lifecycle.onFinish(failure, ctx));
            safeLifecycleCall("onError", () -> lifecycle.onError(failure, ctx));
            return failure;
        }

        long deadlineNs = deadlineMs > 0
            ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMs)
            : Long.MAX_VALUE;

        @SuppressWarnings("unchecked")
        Result<O> result = (Result<O>) executeShared(input, ctx, context, recorder, spanRecorder, traceBuilder, deadlineNs);

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
    // spanRecorder is passed explicitly (not via this.spanRecorder) so the outer pipeline's recorder
    // propagates into branch arm pipelines, which may have been built with a different (or no) recorder.
    Result<?> executeShared(Object input, DefaultStepContext ctx, RequestContext context,
                            MetricsRecorder recorder, SpanRecorder spanRecorder,
                            ExecutionTrace.Builder traceBuilder, long deadlineNs) {
        Object current = input;
        for (EngineNode<?, ?> node : nodes) {
            if (deadlineNs != Long.MAX_VALUE && System.nanoTime() > deadlineNs) {
                return new Failure<>(
                    new PipelineDeadlineExceededException(deadlineMs), "pipeline.deadline", traceBuilder.build());
            }
            if (node instanceof StepNode<?, ?> sn) {
                String stepId = sn.step().describe().id();
                ItemResult result = executeItemWithRetry(sn.step(), current, stepId, ctx, context, recorder, spanRecorder);
                traceBuilder.append(result.trace());
                if (result instanceof ItemFailure f) {
                    return new Failure<>(f.cause(), f.stepId(), traceBuilder.build());
                }
                current = ((ItemSuccess) result).value();
            } else if (node instanceof ParallelNode<?, ?> pn) {
                ParallelOutcome parallelResult = executeParallel(pn, current, ctx, context, recorder, spanRecorder, traceBuilder, deadlineNs);
                if (parallelResult instanceof ParallelFailure pf) {
                    return pf.failure();
                }
                current = ((ParallelSuccess) parallelResult).value();
            } else if (node instanceof BranchNode<?, ?> bn) {
                BranchOutcome branchResult = executeBranch(bn, current, ctx, context, recorder, spanRecorder, traceBuilder, deadlineNs);
                if (branchResult instanceof BranchFailure bf) {
                    return bf.failure();
                }
                current = ((BranchSuccess) branchResult).value();
            } else if (node instanceof ForeachNode<?, ?> fn) {
                ForeachOutcome foreachResult = executeForeach(fn, current, ctx, context, recorder, spanRecorder, traceBuilder, deadlineNs);
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
                                            MetricsRecorder recorder, SpanRecorder spanRecorder) {
        RetryPolicy fpRetry = step.describe().retryPolicy();
        io.flowpipe.api.TimeoutPolicy fpTimeout = step.describe().timeoutPolicy();
        int maxAttempts = fpRetry.maxAttempts();
        long timeoutMs = fpTimeout.timeoutMs();

        // Build Failsafe policy chain in outer-to-inner order:
        // [circuitBreaker (optional)] → [retryPolicy (if maxAttempts > 1)] → [timeout (if set)]
        List<Policy<Object>> policies = new ArrayList<>();

        dev.failsafe.CircuitBreaker<Object> fsCb = circuitBreakers.get(step.describe().id());
        if (fsCb != null) policies.add(fsCb);

        // Declared before the retry block so the onFailedAttempt lambda can capture it.
        // Tracks the highest attempt number for which the listener already emitted step.error,
        // letting the catch block know whether it still needs to emit (avoids double-emit when
        // retries are exhausted, and ensures emission when a retryOn predicate rejects an
        // exception and Failsafe's onFailedAttempt listener is never invoked for that attempt).
        AtomicInteger lastAttemptWithErrorEmitted = new AtomicInteger(0);

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
                lastAttemptWithErrorEmitted.set(attempt);
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
        Object span = safeStartSpan(spanRecorder, itemLabel, context);

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
            safeFinishSpan(spanRecorder, span, StepOutcome.SUCCESS, null);
            return new ItemSuccess(output, new TraceEntry(itemLabel, totalStartNanos, totalDurationNanos, attempt, false));

        } catch (dev.failsafe.CircuitBreakerOpenException cboe) {
            long durationNanos = System.nanoTime() - totalStartNanos;
            Instant retriableAfter = Instant.now().plus(cboe.getCircuitBreaker().getRemainingDelay());
            CircuitBreakerOpenException fpEx = new CircuitBreakerOpenException(itemLabel, retriableAfter);
            emitCircuitOpen(itemLabel, retriableAfter, context);
            emitRecord(recorder, itemLabel, durationNanos, 1, StepOutcome.FAILURE);
            safeFinishSpan(spanRecorder, span, StepOutcome.FAILURE, fpEx);
            return new ItemFailure(fpEx, itemLabel,
                new TraceEntry(itemLabel, totalStartNanos, durationNanos, 1, false));

        } catch (TimeoutExceededException tee) {
            long durationNanos = System.nanoTime() - totalStartNanos;
            int attempt = lastAttempt.get();
            StepTimeoutException ste = new StepTimeoutException(itemLabel, timeoutMs);
            // Emit step.error only if the retry listener hasn't already emitted for this attempt.
            if (lastAttemptWithErrorEmitted.get() < attempt) {
                emitError(itemLabel, attempt, durationNanos, context, ste);
            }
            emitRecord(recorder, itemLabel, durationNanos, attempt, StepOutcome.FAILURE);
            safeFinishSpan(spanRecorder, span, StepOutcome.FAILURE, ste);
            return new ItemFailure(ste, itemLabel,
                new TraceEntry(itemLabel, totalStartNanos, durationNanos, attempt, false));

        } catch (Throwable t) {
            // Unwrap Failsafe wrapper if present so the original exception surfaces in Failure.cause().
            Throwable cause = (t instanceof FailsafeException fe && fe.getCause() != null)
                ? fe.getCause() : t;
            if (cause instanceof InterruptedException) Thread.currentThread().interrupt();
            long durationNanos = System.nanoTime() - totalStartNanos;
            int attempt = lastAttempt.get();
            // Emit step.error only if the retry listener hasn't already emitted for this attempt.
            // This covers: single-attempt fast path, retryOn predicate rejection (listener never
            // fires for the rejected exception), and the final attempt when retries are exhausted
            // (listener already emitted, so we skip here to avoid duplication).
            if (lastAttemptWithErrorEmitted.get() < attempt) {
                emitError(itemLabel, attempt, durationNanos, context, cause);
            }
            emitRecord(recorder, itemLabel, durationNanos, attempt, StepOutcome.FAILURE);
            safeFinishSpan(spanRecorder, span, StepOutcome.FAILURE, cause);
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
                                        SpanRecorder spanRecorder,
                                        ExecutionTrace.Builder traceBuilder, long deadlineNs) {
        String branchId = bn.branchId();
        long startedAtNanos = System.nanoTime();
        boolean selected;
        try {
            selected = bn.predicate().test(input, ctx);
        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startedAtNanos;
            emitError(branchId, 1, durationNanos, context, t);
            emitRecord(recorder, branchId, durationNanos, 1, StepOutcome.FAILURE);
            traceBuilder.append(new TraceEntry(branchId, startedAtNanos, durationNanos, 1, false));
            return new BranchFailure(new Failure<>(t, branchId, traceBuilder.build()));
        }
        long durationNanos = System.nanoTime() - startedAtNanos;
        traceBuilder.append(new TraceEntry(branchId, startedAtNanos, durationNanos, 1, false));

        Pipeline takenArm = selected ? bn.ifTrue() : bn.ifFalse();
        Pipeline skippedArm = selected ? bn.ifFalse() : bn.ifTrue();

        Result<?> armResult = takenArm.executeShared(input, ctx, context, recorder, spanRecorder, traceBuilder, deadlineNs);
        if (armResult instanceof Failure<?> f) {
            return new BranchFailure((Failure<Object>) f);
        }
        Object value = ((Success<?>) armResult).value();

        emitSkipped(skippedArm.nodes, branchId, recorder, spanRecorder, traceBuilder, context);
        return new BranchSuccess(value);
    }

    private static void emitSkipped(List<EngineNode<?, ?>> skippedNodes, String branchId,
                                    MetricsRecorder recorder, SpanRecorder spanRecorder,
                                    ExecutionTrace.Builder traceBuilder, RequestContext context) {
        for (EngineNode<?, ?> node : skippedNodes) {
            if (node instanceof StepNode<?, ?> sn) {
                String stepId = sn.step().describe().id();
                traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                emitSkip(stepId, branchId, context);
                Object span = safeStartSpan(spanRecorder, stepId, context);
                safeFinishSpan(spanRecorder, span, StepOutcome.SKIPPED, null);
            } else if (node instanceof ParallelNode<?, ?> pn) {
                for (Step<?, ?> branch : pn.branches()) {
                    String stepId = branch.describe().id();
                    traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                    emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                    emitSkip(stepId, branchId, context);
                    Object span = safeStartSpan(spanRecorder, stepId, context);
                    safeFinishSpan(spanRecorder, span, StepOutcome.SKIPPED, null);
                }
            } else if (node instanceof BranchNode<?, ?> bn) {
                traceBuilder.append(new TraceEntry(bn.branchId(), 0L, 0L, 0, true));
                emitRecord(recorder, bn.branchId(), 0L, 0, StepOutcome.SKIPPED);
                emitSkip(bn.branchId(), branchId, context);
                Object span = safeStartSpan(spanRecorder, bn.branchId(), context);
                safeFinishSpan(spanRecorder, span, StepOutcome.SKIPPED, null);
                emitSkipped(bn.ifTrue().nodes, branchId, recorder, spanRecorder, traceBuilder, context);
                emitSkipped(bn.ifFalse().nodes, branchId, recorder, spanRecorder, traceBuilder, context);
            } else if (node instanceof ForeachNode<?, ?> fn) {
                String stepId = fn.step().describe().id();
                traceBuilder.append(new TraceEntry(stepId, 0L, 0L, 0, true));
                emitRecord(recorder, stepId, 0L, 0, StepOutcome.SKIPPED);
                emitSkip(stepId, branchId, context);
                Object span = safeStartSpan(spanRecorder, stepId, context);
                safeFinishSpan(spanRecorder, span, StepOutcome.SKIPPED, null);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ParallelOutcome executeParallel(ParallelNode pn, Object input, DefaultStepContext ctx,
                                            RequestContext context, MetricsRecorder recorder,
                                            SpanRecorder spanRecorder,
                                            ExecutionTrace.Builder traceBuilder, long deadlineNs) {
        List<Step<Object, ?>> branches = pn.branches();
        List<Future<ItemResult>> futures = new ArrayList<>(branches.size());

        for (Step<Object, ?> branch : branches) {
            try {
                futures.add(executor.submit(
                    () -> executeItemWithRetry(branch, input, branch.describe().id(), ctx, context, recorder, spanRecorder)));
            } catch (RejectedExecutionException e) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e, branch.describe().id(), traceBuilder.build()));
            }
        }

        List<Object> outputs = new ArrayList<>(branches.size());

        for (Future<ItemResult> future : futures) {
            ItemResult result;
            try {
                if (deadlineNs == Long.MAX_VALUE) {
                    result = future.get();
                } else {
                    long remainingNs = deadlineNs - System.nanoTime();
                    if (remainingNs <= 0) {
                        cancelAll(futures);
                        return new ParallelFailure(new Failure<>(
                            new PipelineDeadlineExceededException(deadlineMs), "pipeline.deadline", traceBuilder.build()));
                    }
                    result = future.get(remainingNs, TimeUnit.NANOSECONDS);
                }
            } catch (TimeoutException e) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(
                    new PipelineDeadlineExceededException(deadlineMs), "pipeline.deadline", traceBuilder.build()));
            } catch (ExecutionException e) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e.getCause(), "unknown", traceBuilder.build()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(e, "interrupted", traceBuilder.build()));
            }

            traceBuilder.append(result.trace());
            if (result instanceof ItemFailure f) {
                cancelAll(futures);
                return new ParallelFailure(new Failure<>(f.cause(), f.stepId(), traceBuilder.build()));
            }
            outputs.add(((ItemSuccess) result).value());
        }

        Object combined;
        if (pn.combiner() != null) {
            combined = pn.combiner().apply(outputs);
            if (combined == null) {
                return new ParallelFailure(new Failure<>(
                    new NullPointerException("parallel combiner returned null; combiner outputs must not be null"),
                    "parallel.combiner", traceBuilder.build()));
            }
        } else {
            // combiner-free: outputs already written to state via each branch's outputKey;
            // the input passes through unchanged as the next pipeline value
            combined = input;
        }
        return new ParallelSuccess(combined);
    }

    @SuppressWarnings("rawtypes")
    private ForeachOutcome executeForeach(ForeachNode fn, Object input, DefaultStepContext ctx,
                                          RequestContext context, MetricsRecorder recorder,
                                          SpanRecorder spanRecorder,
                                          ExecutionTrace.Builder traceBuilder, long deadlineNs) {
        List<?> items = (List<?>) input;
        String stepId = fn.step().describe().id();
        List<Object> outputs = new ArrayList<>(items.size());

        for (int i = 0; i < items.size(); i++) {
            if (deadlineNs != Long.MAX_VALUE && System.nanoTime() > deadlineNs) {
                return new ForeachFailure(new Failure<>(
                    new PipelineDeadlineExceededException(deadlineMs), "pipeline.deadline", traceBuilder.build()));
            }
            if (items.get(i) == null) {
                String itemLabel = stepId + "[" + i + "]";
                return new ForeachFailure(new Failure<>(
                    new NullPointerException("foreach item at index " + i + " is null; list elements must not be null"),
                    itemLabel, traceBuilder.build()));
            }
            String itemLabel = stepId + "[" + i + "]";
            ItemResult result = executeItemWithRetry(fn.step(), items.get(i), itemLabel, ctx, context, recorder, spanRecorder);
            traceBuilder.append(result.trace());
            if (result instanceof ItemFailure f) {
                return new ForeachFailure(new Failure<>(f.cause(), f.stepId(), traceBuilder.build()));
            }
            outputs.add(((ItemSuccess) result).value());
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
        if (output == null) {
            throw new NullPointerException(
                "Step '" + step.describe().id() + "' returned null; step outputs must not be null");
        }
        step.describe().outputValidator().validate(output);
        StateKey outputKey = step.describe().outputKey();
        if (outputKey != null) {
            ctx.state().set(outputKey, output);
        }
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

    private static Object safeStartSpan(SpanRecorder recorder, String stepId, RequestContext context) {
        try {
            return recorder.startStep(stepId, context);
        } catch (Throwable t) {
            LOG.atWarn()
                .setMessage("tracing.recorder_failed")
                .addKeyValue("step.id", stepId)
                .addKeyValue("tracing.op", "startStep")
                .addKeyValue("error.class", t.getClass().getName())
                .addKeyValue("error.message", t.getMessage() == null ? "" : t.getMessage())
                .log();
            return null;
        }
    }

    private static void safeFinishSpan(SpanRecorder recorder, Object span,
                                       StepOutcome outcome, Throwable cause) {
        try {
            recorder.finishStep(span, outcome, cause);
        } catch (Throwable t) {
            LOG.atWarn()
                .setMessage("tracing.recorder_failed")
                .addKeyValue("tracing.op", "finishStep")
                .addKeyValue("error.class", t.getClass().getName())
                .addKeyValue("error.message", t.getMessage() == null ? "" : t.getMessage())
                .log();
        }
    }

}
