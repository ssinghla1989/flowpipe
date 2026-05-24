package io.flowpipe.validation;

@FunctionalInterface
public interface Validator<T> {

    void validate(T value) throws ValidationException;
}
