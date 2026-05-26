package io.flowpipe.example;

import io.flowpipe.api.Failure;
import io.flowpipe.api.PipelineLifecycle;
import io.flowpipe.api.Result;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.engine.PipelineBuilder;
import io.flowpipe.observability.MetricsRecorder;
import io.flowpipe.observability.NoOpMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Builds the order-processing pipeline, wiring all steps and observability.
 *
 * <p>Pipeline structure:
 * <pre>
 *   validate-order
 *     → [check-inventory ‖ calculate-order-total]   (parallel — both receive the validated Order)
 *       → process-payment                            (sequential, with retry — charges orderTotal)
 *         → send-confirmation                        (sequential — reads state, branches on stock)
 *           → log-order                              (sequential — records to persistence)
 * </pre>
 *
 * <p>Key FlowPipe features exercised:
 * <ul>
 *   <li>Sequential and parallel composition</li>
 *   <li>Shared mutable state (Order and OrderContext passed via StateKeys)</li>
 *   <li>Retry policy on process-payment (2 attempts, 100 ms initial delay, 2× backoff)</li>
 *   <li>MetricsRecorder SPI wired around every step</li>
 *   <li>PipelineLifecycle hooks at the pipeline boundary</li>
 * </ul>
 */
public final class OrderProcessingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(OrderProcessingPipeline.class);

    private OrderProcessingPipeline() {}

    public static Pipeline<Order, OrderConfirmation> build(
            InventoryService inventoryService,
            PaymentProcessor paymentProcessor,
            NotificationService notificationService,
            PersistenceService persistenceService) {
        return build(inventoryService, paymentProcessor, notificationService, persistenceService,
            NoOpMetricsRecorder.instance());
    }

    public static Pipeline<Order, OrderConfirmation> build(
            InventoryService inventoryService,
            PaymentProcessor paymentProcessor,
            NotificationService notificationService,
            PersistenceService persistenceService,
            MetricsRecorder metricsRecorder) {
        Objects.requireNonNull(inventoryService, "inventoryService");
        Objects.requireNonNull(paymentProcessor, "paymentProcessor");
        Objects.requireNonNull(notificationService, "notificationService");
        Objects.requireNonNull(persistenceService, "persistenceService");
        Objects.requireNonNull(metricsRecorder, "metricsRecorder");

        return PipelineBuilder.start(Order.class)
            .withMetrics(metricsRecorder)
            .then(OrderProcessingSteps.validateOrder())
            .parallel2(
                OrderContext.class,
                OrderContext::new,
                OrderProcessingSteps.checkInventory(inventoryService),
                OrderProcessingSteps.calculateOrderTotal()
            )
            .then(OrderProcessingSteps.processPayment(paymentProcessor))
            .then(OrderProcessingSteps.sendConfirmation(notificationService))
            .then(OrderProcessingSteps.logOrder(persistenceService))
            .withLifecycle(loggingLifecycle())
            .build();
    }

    private static PipelineLifecycle<Order, OrderConfirmation> loggingLifecycle() {
        return new PipelineLifecycle<>() {
            @Override
            public void onStart(Order input, StepContext ctx) {
                LOG.info("pipeline.start orderId={} customerId={}",
                    input.orderId(), input.customerId());
            }

            @Override
            public void onFinish(Result<OrderConfirmation> result, StepContext ctx) {
                if (result instanceof Success<OrderConfirmation> s) {
                    LOG.info("pipeline.finish orderId={} transactionId={} stockStatus={}",
                        s.value().orderId(), s.value().transactionId(), s.value().stockStatus());
                }
            }

            @Override
            public void onError(Failure<OrderConfirmation> failure, StepContext ctx) {
                LOG.error("pipeline.error failedStepId={}",
                    failure.failedStepId(), failure.cause());
            }
        };
    }
}
