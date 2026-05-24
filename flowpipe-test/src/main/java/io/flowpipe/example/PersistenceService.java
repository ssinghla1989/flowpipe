package io.flowpipe.example;

public interface PersistenceService {
    void log(Order order, OrderConfirmation confirmation) throws Exception;
}
