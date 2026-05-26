package io.flowpipe.state;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class State {

    private final Map<StateKey<?>, Object> values = new ConcurrentHashMap<>();

    public State() {
    }

    public <T> T get(StateKey<T> key) {
        Objects.requireNonNull(key, "key");
        Object raw = values.get(key);
        return raw == null ? null : key.type().cast(raw);
    }

    public <T> void set(StateKey<T> key, T value) {
        Objects.requireNonNull(key, "key");
        if (value == null) {
            values.remove(key);
            return;
        }
        if (!key.type().isInstance(value)) {
            throw new ClassCastException(
                "Value of type " + value.getClass().getName()
                    + " cannot be stored under key " + key);
        }
        values.put(key, value);
    }

    public boolean contains(StateKey<?> key) {
        Objects.requireNonNull(key, "key");
        return values.containsKey(key);
    }

    public int size() {
        return values.size();
    }
}
