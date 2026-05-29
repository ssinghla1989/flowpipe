package io.flowpipe.example;

import io.flowpipe.api.RetryPolicy;
import io.flowpipe.api.Step;
import io.flowpipe.api.StepContext;
import io.flowpipe.api.StepDescriptor;
import io.flowpipe.state.StateKey;

import java.math.BigDecimal;

/**
 * Static factory methods for each step in the order-processing pipeline.
 *
 * <p>Each step demonstrates a distinct FlowPipe capability:
 * <ul>
 *   <li>{@link #validateOrder()} — input validation and shared state write</li>
 *   <li>{@link #checkInventory(InventoryService)} — parallel branch (external service call)</li>
 *   <li>{@link #calculateOrderTotal()} — parallel branch (pure computation, no external I/O)</li>
 *   <li>{@link #processPayment(PaymentProcessor)} — sequential step with retry policy</li>
 *   <li>{@link #sendConfirmation(NotificationService)} — reads multiple values from state</li>
 *   <li>{@link #logOrder(PersistenceService)} — sequential terminal step</li>
 * </ul>
 */
public final class OrderProcessingSteps {

    // Order is stored here by validateOrder() so later steps can access customer/address details
    // even after the pipeline type has advanced past Order (e.g. inside sendConfirmation).
    static final StateKey<Order> ORDER_KEY = StateKey.of("order", Order.class);

    // OrderContext (inventory + total) is stored here by processPayment() so sendConfirmation()
    // can access the inventory status to craft the correct message.
    static final StateKey<OrderContext> ORDER_CONTEXT_KEY =
        StateKey.of("orderContext", OrderContext.class);

    // Payment retries once on transient failure: 2 max attempts, 100 ms initial delay, 2× backoff.
    private static final RetryPolicy PAYMENT_RETRY =
        RetryPolicy.exponential(2, 100L, 30_000L, 2.0, false);

    private OrderProcessingSteps() {}

    /** Validates the order fields and stores it in pipeline state for downstream steps. */
    public static Step<Order, Order> validateOrder() {
        return new Step<>() {
            @Override
            public StepDescriptor<Order, Order> describe() {
                return StepDescriptor.builder("validate-order", Order.class, Order.class).build();
            }

            @Override
            public Order execute(Order input, StepContext ctx) {
                if (input.customerId().isBlank()) {
                    throw new IllegalArgumentException("customerId is blank");
                }
                if (input.deliveryAddress().isBlank()) {
                    throw new IllegalArgumentException("deliveryAddress is blank");
                }
                if (input.items().isEmpty()) {
                    throw new IllegalArgumentException("order has no items");
                }
                for (OrderItem item : input.items()) {
                    if (item.quantity() <= 0) {
                        throw new IllegalArgumentException(
                            "item " + item.productId() + " has invalid quantity: " + item.quantity());
                    }
                }
                ctx.state().set(ORDER_KEY, input);
                return input;
            }
        };
    }

    /**
     * Queries the inventory service for stock availability.
     * Runs in parallel with {@link #calculateOrderTotal()}.
     */
    public static Step<Order, InventoryCheckResult> checkInventory(InventoryService inventoryService) {
        return new Step<>() {
            @Override
            public StepDescriptor<Order, InventoryCheckResult> describe() {
                return StepDescriptor.builder(
                    "check-inventory", Order.class, InventoryCheckResult.class).build();
            }

            @Override
            public InventoryCheckResult execute(Order input, StepContext ctx) throws Exception {
                return inventoryService.check(input);
            }
        };
    }

    /**
     * Computes the order total from item prices and quantities.
     * Runs in parallel with {@link #checkInventory(InventoryService)} — pure computation, no I/O.
     */
    public static Step<Order, BigDecimal> calculateOrderTotal() {
        return new Step<>() {
            @Override
            public StepDescriptor<Order, BigDecimal> describe() {
                return StepDescriptor.builder(
                    "calculate-order-total", Order.class, BigDecimal.class).build();
            }

            @Override
            public BigDecimal execute(Order input, StepContext ctx) {
                return input.items().stream()
                    .map(item -> item.unitPrice().multiply(BigDecimal.valueOf(item.quantity())))
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            }
        };
    }

    /**
     * Charges the customer for the order total. Sequential step run after the parallel block.
     * Retries once on transient failure (2 max attempts, 100 ms initial delay, 2× backoff).
     * Stores the {@link OrderContext} in state so {@link #sendConfirmation} can read inventory status.
     */
    public static Step<OrderContext, PaymentResult> processPayment(PaymentProcessor paymentProcessor) {
        return new Step<>() {
            @Override
            public StepDescriptor<OrderContext, PaymentResult> describe() {
                return StepDescriptor.builder(
                    "process-payment", OrderContext.class, PaymentResult.class)
                    .withRetry(PAYMENT_RETRY)
                    .build();
            }

            @Override
            public PaymentResult execute(OrderContext input, StepContext ctx) throws Exception {
                // Save context so sendConfirmation can access inventory status.
                ctx.state().set(ORDER_CONTEXT_KEY, input);
                Order order = ctx.state().get(ORDER_KEY);
                return paymentProcessor.process(order);
            }
        };
    }

    /**
     * Sends the appropriate confirmation email based on inventory status. Reads the original
     * {@link Order} and {@link OrderContext} from pipeline state.
     */
    public static Step<PaymentResult, OrderConfirmation> sendConfirmation(
            NotificationService notificationService) {
        return new Step<>() {
            @Override
            public StepDescriptor<PaymentResult, OrderConfirmation> describe() {
                return StepDescriptor.builder(
                    "send-confirmation", PaymentResult.class, OrderConfirmation.class).build();
            }

            @Override
            public OrderConfirmation execute(PaymentResult input, StepContext ctx) throws Exception {
                Order order = ctx.state().get(ORDER_KEY);
                OrderContext orderCtx = ctx.state().get(ORDER_CONTEXT_KEY);
                InventoryStatus status = orderCtx.inventory().status();
                String message = switch (status) {
                    case IN_STOCK ->
                        "Your order " + order.orderId()
                            + " has been confirmed and will ship within 2 business days.";
                    case PRE_ORDER ->
                        "Your order " + order.orderId() + " is a pre-order. Estimated delivery: "
                            + orderCtx.inventory().estimatedDelivery();
                    case OUT_OF_STOCK ->
                        "Sorry, your order " + order.orderId()
                            + " could not be fulfilled. Items are out of stock."
                            + " A refund will be initiated.";
                };
                OrderConfirmation confirmation = new OrderConfirmation(
                    order.orderId(),
                    message,
                    status,
                    input.transactionId(),
                    orderCtx.inventory().estimatedDelivery()
                );
                notificationService.send(order, confirmation);
                return confirmation;
            }
        };
    }

    /** Records the completed order to persistence. Reads the {@link Order} from pipeline state. */
    public static Step<OrderConfirmation, OrderConfirmation> logOrder(
            PersistenceService persistenceService) {
        return new Step<>() {
            @Override
            public StepDescriptor<OrderConfirmation, OrderConfirmation> describe() {
                return StepDescriptor.builder(
                    "log-order", OrderConfirmation.class, OrderConfirmation.class).build();
            }

            @Override
            public OrderConfirmation execute(OrderConfirmation input, StepContext ctx) throws Exception {
                Order order = ctx.state().get(ORDER_KEY);
                persistenceService.log(order, input);
                return input;
            }
        };
    }
}
