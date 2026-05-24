dependencies {
    api(project(":flowpipe-core"))
    api(platform("org.junit:junit-bom:5.10.2"))
    api("org.junit.jupiter:junit-jupiter")
    api("org.assertj:assertj-core:3.25.3")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.6")
}

// Run the live demo: ./gradlew :flowpipe-test:runDemo
tasks.register<JavaExec>("runDemo") {
    group = "demo"
    description = "Execute the order-processing pipeline with live HTTP calls, structured logs, and metrics"
    // Use the test runtime classpath so logback + logback-test.xml are on the classpath
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("io.flowpipe.example.OrderProcessingDemo")
}
