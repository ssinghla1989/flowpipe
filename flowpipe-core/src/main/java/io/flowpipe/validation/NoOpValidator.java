package io.flowpipe.validation;

public final class NoOpValidator<T> implements Validator<T> {

    private static final NoOpValidator<Object> INSTANCE = new NoOpValidator<>();

    private NoOpValidator() {
    }

    @SuppressWarnings("unchecked")
    public static <T> NoOpValidator<T> instance() {
        return (NoOpValidator<T>) INSTANCE;
    }

    @Override
    public void validate(T value) {
        // no-op
    }
}
