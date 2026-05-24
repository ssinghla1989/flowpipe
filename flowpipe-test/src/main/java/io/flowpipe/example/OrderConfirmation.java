package io.flowpipe.example;

import java.time.LocalDate;
import java.util.Objects;

public record OrderConfirmation(
    String orderId,
    String message,
    InventoryStatus stockStatus,
    String transactionId,
    LocalDate estimatedDelivery
) {
    public OrderConfirmation {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(stockStatus, "stockStatus");
        Objects.requireNonNull(transactionId, "transactionId");
    }
}
