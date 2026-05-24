package io.flowpipe.example;

import java.math.BigDecimal;
import java.util.Objects;

public record PaymentResult(
    String transactionId,
    BigDecimal amountCharged
) {
    public PaymentResult {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(amountCharged, "amountCharged");
    }
}
