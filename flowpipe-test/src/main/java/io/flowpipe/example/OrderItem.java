package io.flowpipe.example;

import java.math.BigDecimal;
import java.util.Objects;

public record OrderItem(
    String productId,
    int quantity,
    BigDecimal unitPrice
) {
    public OrderItem {
        Objects.requireNonNull(productId, "productId");
        Objects.requireNonNull(unitPrice, "unitPrice");
    }
}
