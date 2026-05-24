package io.flowpipe.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.flowpipe.engine.Pipeline;
import org.slf4j.LoggerFactory;
import org.slf4j.event.KeyValuePair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Slf4jTestAppender extends ListAppender<ILoggingEvent> {

    private final Logger target;
    private final Level previousLevel;

    private Slf4jTestAppender(Logger target) {
        this.target = target;
        this.previousLevel = target.getLevel();
    }

    public static Slf4jTestAppender attachToEngine() {
        Logger logger = (Logger) LoggerFactory.getLogger(Pipeline.class);
        Slf4jTestAppender appender = new Slf4jTestAppender(logger);
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.setLevel(Level.TRACE);
        logger.addAppender(appender);
        return appender;
    }

    public void detach() {
        target.detachAppender(this);
        target.setLevel(previousLevel);
        stop();
        list.clear();
    }

    public synchronized List<ILoggingEvent> events() {
        return Collections.unmodifiableList(new ArrayList<>(list));
    }

    public synchronized List<ILoggingEvent> events(String message) {
        List<ILoggingEvent> filtered = new ArrayList<>();
        for (ILoggingEvent e : list) {
            if (e.getMessage().equals(message)) {
                filtered.add(e);
            }
        }
        return Collections.unmodifiableList(filtered);
    }

    public synchronized void clear() {
        list.clear();
    }

    public static Map<String, Object> fields(ILoggingEvent event) {
        Map<String, Object> map = new HashMap<>();
        List<KeyValuePair> pairs = event.getKeyValuePairs();
        if (pairs != null) {
            for (KeyValuePair p : pairs) {
                map.put(p.key, p.value);
            }
        }
        return map;
    }
}
