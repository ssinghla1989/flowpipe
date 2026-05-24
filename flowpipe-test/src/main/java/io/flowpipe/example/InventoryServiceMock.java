package io.flowpipe.example;

public final class InventoryServiceMock implements InventoryService {

    private final InventoryCheckResult result;

    private InventoryServiceMock(InventoryCheckResult result) {
        this.result = result;
    }

    public static InventoryServiceMock returning(InventoryCheckResult result) {
        return new InventoryServiceMock(result);
    }

    @Override
    public InventoryCheckResult check(Order order) {
        return result;
    }
}
