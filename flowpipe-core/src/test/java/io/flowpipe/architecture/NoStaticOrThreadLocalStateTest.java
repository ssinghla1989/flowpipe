package io.flowpipe.architecture;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class NoStaticOrThreadLocalStateTest {

    @Test
    void core_packages_declare_no_mutable_static_or_thread_local_carriers() throws Exception {
        Path classesRoot = locateCompiledClassesRoot();
        List<Class<?>> classes = loadClassesUnder(classesRoot, "io.flowpipe.");

        List<String> violations = new ArrayList<>();
        for (Class<?> cls : classes) {
            for (Field field : cls.getDeclaredFields()) {
                int mods = field.getModifiers();
                if (!Modifier.isStatic(mods)) continue;
                if (field.isSynthetic()) continue;
                if (ThreadLocal.class.isAssignableFrom(field.getType())) {
                    violations.add("ThreadLocal: " + cls.getName() + "." + field.getName());
                    continue;
                }
                if (!Modifier.isFinal(mods)) {
                    violations.add("non-final static: " + cls.getName() + "." + field.getName());
                }
            }
        }

        assertThat(violations)
            .as("flowpipe-core must not carry execution state via static mutable fields or ThreadLocals")
            .isEmpty();
    }

    private static Path locateCompiledClassesRoot() throws URISyntaxException {
        URL marker = NoStaticOrThreadLocalStateTest.class.getProtectionDomain()
            .getCodeSource().getLocation();
        Path testClasses = Paths.get(marker.toURI());
        // testClasses points at build/classes/java/test; sibling main holds production classes.
        Path mainClasses = testClasses.getParent().resolve("main");
        if (!Files.isDirectory(mainClasses)) {
            throw new IllegalStateException("Could not locate main classes root: " + mainClasses);
        }
        return mainClasses;
    }

    private static List<Class<?>> loadClassesUnder(Path root, String packagePrefix) throws IOException {
        List<Class<?>> result = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(p -> p.toString().endsWith(".class"))
                .forEach(p -> {
                    String relative = root.relativize(p).toString()
                        .replace('/', '.')
                        .replace('\\', '.');
                    String className = relative.substring(0, relative.length() - ".class".length());
                    if (!className.startsWith(packagePrefix)) return;
                    if (className.endsWith("package-info") || className.endsWith("module-info")) return;
                    try {
                        result.add(Class.forName(className, false,
                            NoStaticOrThreadLocalStateTest.class.getClassLoader()));
                    } catch (Throwable t) {
                        throw new RuntimeException("Failed to load " + className, t);
                    }
                });
        }
        return result;
    }
}
