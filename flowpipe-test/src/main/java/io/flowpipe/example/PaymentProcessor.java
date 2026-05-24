package io.flowpipe.example;

public interface PaymentProcessor {
    PaymentResult process(Order order) throws Exception;
}
