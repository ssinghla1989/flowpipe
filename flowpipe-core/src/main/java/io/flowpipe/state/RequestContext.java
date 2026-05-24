package io.flowpipe.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class RequestContext {

    private static final RequestContext EMPTY = new RequestContext(Collections.emptyMap());

    private final Map<ContextKey<?>, Object> values;

    private RequestContext(Map<ContextKey<?>, Object> values) {
        this.values = values;
    }

    public static RequestContext empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public <T> T get(ContextKey<T> key) {
        Objects.requireNonNull(key, "key");
        Object raw = values.get(key);
        return raw == null ? null : key.type().cast(raw);
    }

    public boolean contains(ContextKey<?> key) {
        Objects.requireNonNull(key, "key");
        return values.containsKey(key);
    }

    public int size() {
        return values.size();
    }

    public void forEach(BiConsumer<ContextKey<?>, Object> action) {
        Objects.requireNonNull(action, "action");
        values.forEach(action);
    }

    public static final class Builder {
        private final Map<ContextKey<?>, Object> values = new HashMap<>();

        private Builder() {
        }

        public <T> Builder put(ContextKey<T> key, T value) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            if (!key.type().isInstance(value)) {
                throw new ClassCastException(
                    "Value of type " + value.getClass().getName()
                        + " cannot be stored under key " + key);
            }
            values.put(key, value);
            return this;
        }

        public RequestContext build() {
            if (values.isEmpty()) {
                return EMPTY;
            }
            return new RequestContext(Collections.unmodifiableMap(new HashMap<>(values)));
        }
    }
}
