package io.flowpipe.test;

import io.flowpipe.api.Step;

public final class Steps {

    private Steps() {
    }

    public static <T> Step<T, T> identity(String id, Class<T> type) {
        return Step.of(id, type, type, (input, ctx) -> input);
    }

    public static <T> Step<T, T> throwing(String id, Class<T> type, RuntimeException error) {
        return Step.of(id, type, type, (input, ctx) -> {
            throw error;
        });
    }

    public static Step<Void, Void> noop(String id) {
        return Step.of(id, Void.class, Void.class, (input, ctx) -> null);
    }
}
