package io.flowpipe.example;

public interface InventoryService {
    InventoryCheckResult check(Order order) throws Exception;
}
