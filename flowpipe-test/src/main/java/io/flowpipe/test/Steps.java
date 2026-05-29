package io.flowpipe.test;

import io.flowpipe.api.Step;

import java.util.List;

public final class Steps {

    private Steps() {
    }

    public static <T> Step<T, T> identity(String id, Class<T> type) {
        return Step.builder(id, type, type).execute((input, ctx) -> input).build();
    }

    public static <T> Step<T, T> throwing(String id, Class<T> type, RuntimeException error) {
        return Step.builder(id, type, type).execute((input, ctx) -> {
            throw error;
        }).build();
    }

    public static Step<Void, Void> noop(String id) {
        return Step.builder(id, Void.class, Void.class).execute((input, ctx) -> null).build();
    }

    /**
     * A step that splits a delimited string into a list of trimmed tokens.
     * Useful for feeding downstream {@code foreach} steps in tests.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Step<String, List<String>> split(String id, String delimiter) {
        return (Step<String, List<String>>) (Step) Step.builder(id, String.class, List.class).execute((input, ctx) -> List.of(input.split(delimiter))).build();
    }

    /**
     * A per-element step that uppercases a string — intended for use inside {@code foreach}.
     */
    public static Step<String, String> uppercase(String id) {
        return Step.builder(id, String.class, String.class).execute((input, ctx) -> input.toUpperCase()).build();
    }
}
