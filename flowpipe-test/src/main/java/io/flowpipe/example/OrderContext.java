package io.flowpipe.example;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Combines the results of the parallel inventory check and order-total calculation,
 * flowing into the sequential processPayment step.
 */
public record OrderContext(
    InventoryCheckResult inventory,
    BigDecimal orderTotal
) {
    public OrderContext {
        Objects.requireNonNull(inventory, "inventory");
        Objects.requireNonNull(orderTotal, "orderTotal");
    }
}
