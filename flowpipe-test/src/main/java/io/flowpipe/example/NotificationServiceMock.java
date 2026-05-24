package io.flowpipe.example;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class NotificationServiceMock implements NotificationService {

    private final List<OrderConfirmation> sentConfirmations = new ArrayList<>();

    @Override
    public synchronized void send(Order order, OrderConfirmation confirmation) {
        sentConfirmations.add(confirmation);
    }

    public synchronized List<OrderConfirmation> getSentConfirmations() {
        return Collections.unmodifiableList(new ArrayList<>(sentConfirmations));
    }

    public synchronized boolean hasReceivedAnyConfirmation() {
        return !sentConfirmations.isEmpty();
    }
}
