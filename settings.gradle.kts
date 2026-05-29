plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "flowpipe"

include("flowpipe-core")
include("flowpipe-test")
include("gpc-commons")
