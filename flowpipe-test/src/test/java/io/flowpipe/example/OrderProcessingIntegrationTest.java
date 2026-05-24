package io.flowpipe.example;

import io.flowpipe.api.Failure;
import io.flowpipe.api.Result;
import io.flowpipe.api.Success;
import io.flowpipe.engine.Pipeline;
import io.flowpipe.observability.StepOutcome;
import io.flowpipe.test.RecordingMetricsRecorder;
import io.flowpipe.test.RecordingMetricsRecorder.OutcomeEvent;
import io.flowpipe.test.RecordingMetricsRecorder.RetryAttemptEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrderProcessingIntegrationTest {

    private InventoryServiceMock inventoryService;
    private PaymentProcessorMock paymentProcessor;
    private NotificationServiceMock notificationService;
    private PersistenceServiceMock persistenceService;
    private RecordingMetricsRecorder metrics;

    @BeforeEach
    void setUp() {
        inventoryService = InventoryServiceMock.returning(
            new InventoryCheckResult(InventoryStatus.IN_STOCK, null));
        paymentProcessor = PaymentProcessorMock.alwaysSucceeds("txn-001");
        notificationService = new NotificationServiceMock();
        persistenceService = new PersistenceServiceMock();
        metrics = new RecordingMetricsRecorder();
    }

    private Order sampleOrder() {
        return new Order(
            "order-123",
            "customer-456",
            List.of(new OrderItem("product-789", 2, new BigDecimal("19.99"))),
            "123 Main St, Anytown"
        );
    }

    private Pipeline<Order, OrderConfirmation> buildPipeline() {
        return OrderProcessingPipeline.build(
            inventoryService, paymentProcessor, notificationService, persistenceService, metrics);
    }

    // --- Happy path tests (tasks 5.2, 5.3) ---

    @Test
    void happy_path_in_stock_sends_shipping_confirmation() {
        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);
        OrderConfirmation confirmation = ((Success<OrderConfirmation>) result).value();
        assertThat(confirmation.orderId()).isEqualTo("order-123");
        assertThat(confirmation.stockStatus()).isEqualTo(InventoryStatus.IN_STOCK);
        assertThat(confirmation.transactionId()).isEqualTo("txn-001");
        assertThat(confirmation.message()).contains("will ship");

        assertThat(notificationService.hasReceivedAnyConfirmation()).isTrue();
        assertThat(persistenceService.getLoggedOrders()).hasSize(1);
    }

    @Test
    void happy_path_pre_order_sends_preorder_confirmation() {
        LocalDate estimatedDelivery = LocalDate.now().plusDays(30);
        inventoryService = InventoryServiceMock.returning(
            new InventoryCheckResult(InventoryStatus.PRE_ORDER, estimatedDelivery));

        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);
        OrderConfirmation confirmation = ((Success<OrderConfirmation>) result).value();
        assertThat(confirmation.stockStatus()).isEqualTo(InventoryStatus.PRE_ORDER);
        assertThat(confirmation.estimatedDelivery()).isEqualTo(estimatedDelivery);
        assertThat(confirmation.message()).contains("pre-order");

        assertThat(notificationService.hasReceivedAnyConfirmation()).isTrue();
        assertThat(persistenceService.getLoggedOrders()).hasSize(1);
    }

    // --- Metrics assertions (tasks 5.4, 5.5) ---

    @Test
    void successful_execution_records_success_metrics_for_all_steps() {
        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);

        for (String stepId : List.of("validate-order", "check-inventory",
                "calculate-order-total", "process-payment", "send-confirmation", "log-order")) {
            assertThat(metrics.events(stepId))
                .as("expected SUCCESS outcome event for step: %s", stepId)
                .filteredOn(e -> e instanceof OutcomeEvent oe && oe.outcome() == StepOutcome.SUCCESS)
                .isNotEmpty();
        }
    }

    // --- Failure scenario tests (tasks 6.1–6.4) ---

    @Test
    void blank_customer_id_fails_at_validation_without_executing_downstream_steps() {
        Order invalid = new Order(
            "order-123", "", List.of(new OrderItem("prod-1", 1, BigDecimal.TEN)), "123 Main St");

        Result<OrderConfirmation> result = buildPipeline().execute(invalid);

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<OrderConfirmation>) result).failedStepId()).isEqualTo("validate-order");

        assertThat(notificationService.hasReceivedAnyConfirmation()).isFalse();
        assertThat(persistenceService.getLoggedOrders()).isEmpty();
    }

    @Test
    void validation_failure_records_failure_metric_and_no_downstream_metrics() {
        Order invalid = new Order(
            "order-123", "", List.of(new OrderItem("prod-1", 1, BigDecimal.TEN)), "123 Main St");

        buildPipeline().execute(invalid);

        assertThat(metrics.events("validate-order"))
            .filteredOn(e -> e instanceof OutcomeEvent oe && oe.outcome() == StepOutcome.FAILURE)
            .isNotEmpty();

        // downstream steps must not have executed
        assertThat(metrics.events("check-inventory")).isEmpty();
        assertThat(metrics.events("process-payment")).isEmpty();
    }

    @Test
    void out_of_stock_inventory_succeeds_and_sends_out_of_stock_notification() {
        inventoryService = InventoryServiceMock.returning(
            new InventoryCheckResult(InventoryStatus.OUT_OF_STOCK, null));

        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);
        OrderConfirmation confirmation = ((Success<OrderConfirmation>) result).value();
        assertThat(confirmation.stockStatus()).isEqualTo(InventoryStatus.OUT_OF_STOCK);
        assertThat(confirmation.message()).contains("out of stock");

        assertThat(notificationService.hasReceivedAnyConfirmation()).isTrue();
        assertThat(persistenceService.getLoggedOrders()).hasSize(1);
    }

    @Test
    void permanent_payment_failure_fails_pipeline_and_sends_no_notification() {
        paymentProcessor = PaymentProcessorMock.alwaysFails("payment declined");

        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<OrderConfirmation>) result).failedStepId()).isEqualTo("process-payment");

        assertThat(notificationService.hasReceivedAnyConfirmation()).isFalse();
        assertThat(persistenceService.getLoggedOrders()).isEmpty();
    }

    // --- Retry tests (tasks 7.1–7.4) ---

    @Test
    void transient_payment_failure_is_retried_and_pipeline_succeeds() {
        paymentProcessor = PaymentProcessorMock.failsOnceThenSucceeds("txn-retry-001");

        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);
        assertThat(((Success<OrderConfirmation>) result).value().transactionId())
            .isEqualTo("txn-retry-001");

        // exactly 2 calls: one failed attempt + one successful retry
        assertThat(paymentProcessor.getCallCount()).isEqualTo(2);

        // retry metric was recorded by the framework
        assertThat(metrics.events("process-payment"))
            .filteredOn(e -> e instanceof RetryAttemptEvent)
            .isNotEmpty();

        // pipeline completed normally despite the transient failure
        assertThat(notificationService.hasReceivedAnyConfirmation()).isTrue();
        assertThat(persistenceService.getLoggedOrders()).hasSize(1);
    }

    // --- Concurrency and edge case tests (tasks 8.1–8.4) ---

    @Test
    void parallel_inventory_check_and_order_total_both_execute_independently() {
        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);

        // Both parallel branches recorded metrics, confirming independent execution
        assertThat(metrics.events("check-inventory")).isNotEmpty();
        assertThat(metrics.events("calculate-order-total")).isNotEmpty();
    }

    @Test
    void multiple_consecutive_executions_maintain_state_isolation() {
        Pipeline<Order, OrderConfirmation> pipeline = buildPipeline();

        Order order1 = new Order(
            "order-001", "customer-A", List.of(new OrderItem("prod-1", 1, BigDecimal.ONE)), "Address A");
        Order order2 = new Order(
            "order-002", "customer-B", List.of(new OrderItem("prod-2", 3, BigDecimal.TEN)), "Address B");

        Result<OrderConfirmation> result1 = pipeline.execute(order1);
        Result<OrderConfirmation> result2 = pipeline.execute(order2);

        assertThat(result1).isInstanceOf(Success.class);
        assertThat(result2).isInstanceOf(Success.class);

        assertThat(((Success<OrderConfirmation>) result1).value().orderId()).isEqualTo("order-001");
        assertThat(((Success<OrderConfirmation>) result2).value().orderId()).isEqualTo("order-002");

        // Both orders independently logged
        assertThat(persistenceService.getLoggedOrders()).hasSize(2);
    }

    @Test
    void order_with_no_items_fails_validation() {
        Order empty = new Order("order-123", "customer-456", List.of(), "123 Main St");

        Result<OrderConfirmation> result = buildPipeline().execute(empty);

        assertThat(result).isInstanceOf(Failure.class);
        assertThat(((Failure<OrderConfirmation>) result).failedStepId()).isEqualTo("validate-order");
    }

    @Test
    void parallel_execution_records_success_metrics_for_both_branches() {
        Result<OrderConfirmation> result = buildPipeline().execute(sampleOrder());

        assertThat(result).isInstanceOf(Success.class);

        assertThat(metrics.events("check-inventory"))
            .filteredOn(e -> e instanceof OutcomeEvent oe && oe.outcome() == StepOutcome.SUCCESS)
            .isNotEmpty();
        assertThat(metrics.events("calculate-order-total"))
            .filteredOn(e -> e instanceof OutcomeEvent oe && oe.outcome() == StepOutcome.SUCCESS)
            .isNotEmpty();
    }
}
