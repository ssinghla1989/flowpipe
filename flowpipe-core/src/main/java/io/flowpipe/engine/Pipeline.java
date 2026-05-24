package io.flowpipe.engine;

import io.flowpipe.api.ExecutionTrace;
import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.TraceEntry;
import io.flowpipe.api.Success;
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.state.RequestContext;
import io.flowpipe.state.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class Pipeline<I, O> {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private final Class<I> inputType;
    private final Class<O> outputType;
    private final List<EngineNode<?, ?>> nodes;
    private final MetricsRecorder defaultRecorder;
    private final ExecutorService executor;
    private final PipelineLifecycle<I, O> lifecycle;

    Pipeline(Class<I> inputType,
             Class<O> outputType,
             List<EngineNode<?, ?>> nodes,
             MetricsRecorder defaultRecorder,
             ExecutorService executor,
             PipelineLifecycle<I, O> lifecycle) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.nodes = nodes;
        this.defaultRecorder = defaultRecorder;
        this.executor = executor;
        this.lifecycle = lifecycle;
    }

    public Class<I> inputType() {
        return inputType;
    }

    public Class<O> outputType() {
        return outputType;
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
                RetryPolicy policy = sn.step().describe().retryPolicy();
                int maxAttempts = policy.maxAttempts();
                long totalStartNanos = System.nanoTime();
                Throwable lastThrowable = null;
                Object output = null;
                boolean succeeded = false;

                for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                    emitStart(stepId, attempt, context);
                    long attemptStartNanos = System.nanoTime();
                    try {
                        output = invokeStep(sn.step(), current, ctx);
                        long totalDurationNanos = System.nanoTime() - totalStartNanos;
                        traceBuilder.append(new TraceEntry(stepId, totalStartNanos, totalDurationNanos, attempt, false));
                        emitRecord(recorder, stepId, totalDurationNanos, attempt, StepOutcome.SUCCESS);
                        emitFinish(stepId, attempt, totalDurationNanos, context);
                        succeeded = true;
                        break;
                    } catch (Throwable t) {
                        lastThrowable = t;
                        long attemptDurationNanos = System.nanoTime() - attemptStartNanos;
                        emitError(stepId, attempt, attemptDurationNanos, context, t);
                        if (attempt < maxAttempts) {
                            long delayMs = computeDelay(policy, attempt);
                            emitRetry(stepId, attempt, maxAttempts, delayMs, context);
                            final int capturedAttempt = attempt;
                            safeRecord(stepId, "recordRetryAttempt",
                                () -> recorder.recordRetryAttempt(stepId, capturedAttempt));
                            sleepMillis(delayMs);
                        }
                    }
                }

                if (!succeeded) {
                    long totalDurationNanos = System.nanoTime() - totalStartNanos;
                    traceBuilder.append(new TraceEntry(stepId, totalStartNanos, totalDurationNanos, maxAttempts, false));
                    emitRecord(recorder, stepId, totalDurationNanos, maxAttempts, StepOutcome.FAILURE);
                    return new Failure<>(lastThrowable, stepId, traceBuilder.build());
                }
                current = output;
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
            }
        }
        return new Success<>(current, traceBuilder.build());
    }

    private sealed interface ParallelOutcome permits ParallelSuccess, ParallelFailure {}
    private record ParallelSuccess(Object value) implements ParallelOutcome {}
    private record ParallelFailure(Failure<Object> failure) implements ParallelOutcome {}

    private sealed interface BranchOutcome permits BranchSuccess, BranchFailure {}
    private record BranchSuccess(Object value) implements BranchOutcome {}
    private record BranchFailure(Failure<Object> failure) implements BranchOutcome {}

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

    private static void cancelAll(List<Future<BranchResult>> futures) {
        for (Future<BranchResult> f : futures) {
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

    private static long computeDelay(RetryPolicy policy, int failedAttempt) {
        long base = (long) Math.floor(policy.initialDelayMs() * Math.pow(policy.multiplier(), failedAttempt - 1));
        if (policy.jitter() && base > 0) {
            base = (long) (Math.random() * base);
        }
        return base;
    }

    private static void sleepMillis(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
