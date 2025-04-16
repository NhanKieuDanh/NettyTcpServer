plugins {
    kotlin("jvm") version "1.9.24"
    application
}

group = "com.example.netty"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.netty:netty-all:4.1.100.Final")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.example.netty.MainKt")
}