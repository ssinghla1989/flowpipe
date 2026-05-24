package io.flowpipe.example;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.test.RecordingMetricsRecorder;
import io.flowpipe.test.RecordingMetricsRecorder.AttemptsEvent;
import io.flowpipe.test.RecordingMetricsRecorder.DurationEvent;
import io.flowpipe.test.RecordingMetricsRecorder.OutcomeEvent;
import io.flowpipe.test.RecordingMetricsRecorder.RetryAttemptEvent;

import java.math.BigDecimal;
import java.util.List;

/**
 * Runnable demo that executes the order-processing pipeline against live public APIs.
 *
 * <p>Run with: {@code ./gradlew :flowpipe-test:runDemo}
 *
 * <p>External APIs used:
 * <ul>
 *   <li>GET  https://dummyjson.com/products/{id}       — inventory / stock check</li>
 *   <li>POST https://jsonplaceholder.typicode.com/posts — payment, notification, persistence</li>
 * </ul>
 */
public final class OrderProcessingDemo {

    public static void main(String[] args) throws Exception {
        // DummyJSON product 4 = "OPPOF19" (stock varies — good for seeing PRE_ORDER or IN_STOCK)
        Order order = new Order(
            "order-" + System.currentTimeMillis(),
            "customer-42",
            List.of(new OrderItem("4", 1, new BigDecimal("189.99"))),
            "221B Baker Street, London, UK"
        );

        line();
        System.out.println("  FlowPipe Order Processing Demo — live HTTP calls");
        line();
        System.out.println("  Order:    " + order.orderId());
        System.out.println("  Customer: " + order.customerId());
        System.out.println("  Product:  dummyjson.com/products/" + order.items().get(0).productId());
        System.out.println("  Address:  " + order.deliveryAddress());
        line();
        System.out.println();

        var inventoryService  = new HttpInventoryService();
        var paymentProcessor  = new HttpPaymentProcessor();
        var notificationService = new HttpNotificationService();
        var persistenceService  = new HttpPersistenceService();
        var metrics = new RecordingMetricsRecorder();

        var pipeline = OrderProcessingPipeline.build(
            inventoryService, paymentProcessor, notificationService, persistenceService, metrics);

        System.out.println("  Executing pipeline...");
        System.out.println("  (FlowPipe structured logs appear below)");
        System.out.println();

        Result<OrderConfirmation> result = pipeline.execute(order);

        System.out.println();
        line();
        System.out.println("  Result");
        line();
        if (result instanceof Success<OrderConfirmation> s) {
            OrderConfirmation c = s.value();
            System.out.println("  Status:      SUCCESS");
            System.out.println("  Order ID:    " + c.orderId());
            System.out.println("  Transaction: " + c.transactionId());
            System.out.println("  Stock:       " + c.stockStatus());
            if (c.estimatedDelivery() != null) {
                System.out.println("  Est. Delivery: " + c.estimatedDelivery());
            }
            System.out.println("  Message:     " + c.message());
        } else if (result instanceof Failure<OrderConfirmation> f) {
            System.out.println("  Status:      FAILED");
            System.out.println("  Failed step: " + f.failedStepId());
            System.out.println("  Cause:       " + f.cause().getMessage());
        }

        System.out.println();
        line();
        System.out.println("  Step Metrics");
        line();
        System.out.printf("  %-32s %-10s %7s   %s%n", "STEP", "OUTCOME", "MS", "ATTEMPTS");
        System.out.printf("  %-32s %-10s %7s   %s%n", "----", "-------", "--", "--------");

        for (String stepId : List.of(
                "validate-order", "check-inventory", "calculate-order-total",
                "process-payment", "send-confirmation", "log-order")) {
            List<RecordingMetricsRecorder.Event> events = metrics.events(stepId);
            if (events.isEmpty()) {
                System.out.printf("  %-32s %-10s%n", stepId, "skipped");
                continue;
            }

            StepOutcome outcome = events.stream()
                .filter(e -> e instanceof OutcomeEvent)
                .map(e -> ((OutcomeEvent) e).outcome())
                .findFirst()
                .orElse(null);

            long durationMs = events.stream()
                .filter(e -> e instanceof DurationEvent)
                .map(e -> ((DurationEvent) e).durationNanos() / 1_000_000)
                .findFirst()
                .orElse(-1L);

            int attempts = events.stream()
                .filter(e -> e instanceof AttemptsEvent)
                .map(e -> ((AttemptsEvent) e).attempts())
                .findFirst()
                .orElse(1);

            long retries = events.stream()
                .filter(e -> e instanceof RetryAttemptEvent)
                .count();

            String attemptsLabel = attempts == 1 ? "1" : attempts + " (retried " + retries + "×)";
            System.out.printf("  %-32s %-10s %7d   %s%n",
                stepId, outcome, durationMs, attemptsLabel);
        }

        line();
        System.out.println();
    }

    private static void line() {
        System.out.println("  " + "─".repeat(60));
    }
}
