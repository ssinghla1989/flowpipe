package io.flowpipe.example;

import java.time.LocalDate;
import java.util.Objects;

public record InventoryCheckResult(
    InventoryStatus status,
    LocalDate estimatedDelivery
) {
    public InventoryCheckResult {
        Objects.requireNonNull(status, "status");
    }
}
