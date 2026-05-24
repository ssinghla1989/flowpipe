package io.flowpipe.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PersistenceServiceMock implements PersistenceService {

    private final List<OrderConfirmation> loggedOrders = new ArrayList<>();

    @Override
    public synchronized void log(Order order, OrderConfirmation confirmation) {
        loggedOrders.add(confirmation);
    }

    public synchronized List<OrderConfirmation> getLoggedOrders() {
        return Collections.unmodifiableList(new ArrayList<>(loggedOrders));
    }
}
