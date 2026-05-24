package io.flowpipe.example;

import java.util.List;
import java.util.Objects;

public record Order(
    String orderId,
    String customerId,
    List<OrderItem> items,
    String deliveryAddress
) {
    public Order {
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(customerId, "customerId");
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(deliveryAddress, "deliveryAddress");
        items = List.copyOf(items);
    }
}
