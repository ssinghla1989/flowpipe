package io.flowpipe.test;

import io.flowpipe.api.Step;

import java.util.List;

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

    /**
     * A step that splits a delimited string into a list of trimmed tokens.
     * Useful for feeding downstream {@code foreach} steps in tests.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Step<String, List<String>> split(String id, String delimiter) {
        return (Step<String, List<String>>) (Step) Step.of(id, String.class, List.class,
            (input, ctx) -> List.of(input.split(delimiter)));
    }

    /**
     * A per-element step that uppercases a string — intended for use inside {@code foreach}.
     */
    public static Step<String, String> uppercase(String id) {
        return Step.of(id, String.class, String.class, (input, ctx) -> input.toUpperCase());
    }
}
