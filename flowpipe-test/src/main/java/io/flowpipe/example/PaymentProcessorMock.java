package io.flowpipe.example;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

public final class PaymentProcessorMock implements PaymentProcessor {

    private final boolean alwaysFail;
    private final int failCount;
    private final RuntimeException error;
    private final PaymentResult successResult;
    private final AtomicInteger callCount = new AtomicInteger();

    private PaymentProcessorMock(boolean alwaysFail, int failCount,
                                  RuntimeException error, PaymentResult successResult) {
        this.alwaysFail = alwaysFail;
        this.failCount = failCount;
        this.error = error;
        this.successResult = successResult;
    }

    public static PaymentProcessorMock alwaysSucceeds(String transactionId) {
        return new PaymentProcessorMock(false, 0, null,
            new PaymentResult(transactionId, BigDecimal.TEN));
    }

    public static PaymentProcessorMock failsOnceThenSucceeds(String transactionId) {
        return new PaymentProcessorMock(false, 1,
            new RuntimeException("transient payment error"),
            new PaymentResult(transactionId, BigDecimal.TEN));
    }

    public static PaymentProcessorMock alwaysFails(String errorMessage) {
        return new PaymentProcessorMock(true, 0,
            new RuntimeException(errorMessage), null);
    }

    @Override
    public PaymentResult process(Order order) {
        int count = callCount.incrementAndGet();
        if (alwaysFail || count <= failCount) {
            throw error;
        }
        return successResult;
    }

    public int getCallCount() {
        return callCount.get();
    }
}
