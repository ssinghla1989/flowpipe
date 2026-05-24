package io.flowpipe.example;

public interface NotificationService {
    void send(Order order, OrderConfirmation confirmation) throws Exception;
}
