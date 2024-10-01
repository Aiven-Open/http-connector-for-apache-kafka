import com.diffplug.spotless.extra.wtp.EclipseWtpFormatterStep
import com.github.spotbugs.snom.SpotBugsTask
import java.io.FileOutputStream
import java.net.URL

/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    // https://docs.gradle.org/current/userguide/java_library_plugin.html
    `java-library`

    // https://docs.gradle.org/current/userguide/distribution_plugin.html
    distribution

    // https://docs.gradle.org/current/userguide/checkstyle_plugin.html
    checkstyle

    // https://docs.gradle.org/current/userguide/idea_plugin.html
    idea

    // https://plugins.gradle.org/plugin/com.github.spotbugs
    id("com.github.spotbugs") version "6.0.9"

    // https://plugins.gradle.org/plugin/com.diffplug.gradle.spotless
    id("com.diffplug.spotless") version "6.25.0"
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.wrapper {
    distributionType = Wrapper.DistributionType.ALL
    doLast {
        val sha256Sum = URL("$distributionUrl.sha256").readText()
        propertiesFile.appendText("distributionSha256Sum=${sha256Sum}\n")
        println("Added checksum to wrapper properties")
    }
}

val moduleName = "io.aiven.kafka.connect.http"

val avroVersion = "1.9.2" // Version 1.9.2 brings Jackson 2.10.2 package for Avro
val confluentPlatformVersion = "6.0.2" // For compatibility tests use version 6.0.2 to match Kafka 2.6.
// NOTE: Confluent Platform v6.0.3 has a dependency mismatch issue.
val hamcrestVersion = "2.2"
val jacksonVersion = "2.17.2" // This Jackson is used in the tests.
val jupiterVersion = "5.11.0"
val kafkaVersion = "2.6.3"
val jettyVersion = "9.4.51.v20230217"
val junit4Version = "4.13.2"
val jsr305Version = "3.0.2"
val log4jVersion = "2.24.1"
val mockitoVersion = "5.14.1"
val servletVersion = "4.0.1"
val slf4japiVersion = "1.7.36"
val spotbugsAnnotationsVersion = "4.8.6"
val testcontainersVersion = "1.20.2"
val assertjVersion = "3.25.3"
val awaitilityVersion = "4.2.2"

distributions {
    named("main") {
        contents {
            from(tasks.jar)
            from(configurations.runtimeClasspath)
        }
    }
}

sourceSets {
    create("integrationTest") {
        java.srcDir(file("src/integration-test/java"))
        resources.srcDir(file("src/integration-test/resources"))
        compileClasspath += sourceSets.main.get().output + configurations.testRuntimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

idea {
    module {
        testSources.from(project.sourceSets["integrationTest"].java.srcDirs)
        testSources.from(project.sourceSets["integrationTest"].resources.srcDirs)
    }
}

val integrationTestImplementation: Configuration by configurations.getting {
    extendsFrom(configurations["testImplementation"])
}

val integrationTestRuntimeOnly: Configuration by configurations.getting {
    extendsFrom(configurations["testRuntimeOnly"])
}

dependencies {
    compileOnly("org.apache.kafka:connect-api:$kafkaVersion")
    compileOnly("org.apache.kafka:connect-json:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-clients:$kafkaVersion")

    // For Spotbugs suppressions
    compileOnly("com.github.spotbugs:spotbugs-annotations:$spotbugsAnnotationsVersion")
    compileOnly("com.google.code.findbugs:jsr305:$jsr305Version")

    implementation("com.fasterxml.jackson.core:jackson-core:$jacksonVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("org.slf4j:slf4j-api:$slf4japiVersion")

    testRuntimeOnly("org.apache.kafka:connect-json:$kafkaVersion")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter:$jupiterVersion")

    testImplementation("org.apache.kafka:connect-api:$kafkaVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$jupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$jupiterVersion")
    testImplementation("org.mockito:mockito-core:$mockitoVersion")
    testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")

    integrationTestRuntimeOnly("io.confluent:kafka-avro-serializer:$confluentPlatformVersion")
    integrationTestRuntimeOnly("io.confluent:kafka-connect-avro-converter:$confluentPlatformVersion")
    integrationTestRuntimeOnly("io.confluent:kafka-json-serializer:$confluentPlatformVersion")
    integrationTestRuntimeOnly("org.junit.jupiter:junit-jupiter:$jupiterVersion")
    integrationTestRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")

    integrationTestImplementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    integrationTestImplementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    integrationTestImplementation("javax.servlet:javax.servlet-api:$servletVersion")
    integrationTestImplementation("org.apache.avro:avro:$avroVersion")
    integrationTestImplementation("org.apache.kafka:connect-runtime:$kafkaVersion")
    integrationTestImplementation("org.eclipse.jetty:jetty-http:$jettyVersion")
    integrationTestImplementation("org.eclipse.jetty:jetty-server:$jettyVersion")
    integrationTestImplementation("org.eclipse.jetty:jetty-util:$jettyVersion")
    integrationTestImplementation("junit:junit:$junit4Version") // This is for testcontainers
    integrationTestImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:kafka:$testcontainersVersion") // this is not Kafka version
    integrationTestImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    integrationTestImplementation("org.awaitility:awaitility:$awaitilityVersion")

    // Make test utils from "test" available in "integration-test"
    integrationTestImplementation(sourceSets["test"].output)
}

checkstyle {
    toolVersion = "8.25"
}

tasks.compileJava {
    inputs.property("moduleName", moduleName)
    doFirst {
        options.compilerArgs = listOf("--module-path", classpath.asPath)
        classpath = files()
    }
}

tasks {
    create("integrationTest", Test::class) {
        description = "Runs the integration tests."
        group = "verification"
        testClassesDirs = sourceSets["integrationTest"].output.classesDirs
        classpath = sourceSets["integrationTest"].runtimeClasspath
        dependsOn("test", "distTar")
        useJUnitPlatform()
        outputs.upToDateWhen { false }
        systemProperty("integration-test.distribution.file.path", distTar.get().archiveFile.get().asFile.path)
    }
}

tasks.test {
    useJUnitPlatform {
        includeEngines("junit-jupiter")
    }
}

tasks.processResources {
    filesMatching("http-connector-for-apache-kafka-version.properties") {
        expand(mapOf("version" to version))
    }
}

tasks.jar {
    manifest {
        attributes(mapOf("Version" to archiveVersion.get()))
    }
}

tasks.register("connectorConfigDoc") {
    description = "Generates the connector's configuration documentation."
    group = "documentation"
    dependsOn("classes")
    doLast {
        project.javaexec {
            mainClass = "io.aiven.kafka.connect.http.config.HttpSinkConfig"
            classpath = sourceSets.main.get().runtimeClasspath + sourceSets.main.get().compileClasspath
            standardOutput = FileOutputStream("$projectDir/docs/sink-connector-config-options.rst")
        }
    }
}

spotless {
    format("xml") {
        target("**/*.xml")
        targetExclude(".*/**", "gradle/wrapper/**")
        eclipseWtp(EclipseWtpFormatterStep.XML).configFile("${project.rootDir}/config/spotless-eclipse-wtp-xml.prefs")
        endWithNewline()
    }

    format("misc") {
        target("**/*.gradle", "**/*.md", "**/*.properties")
        targetExclude(".*/**", "gradle/wrapper/**")
        endWithNewline()
        indentWithSpaces()
        trimTrailingWhitespace()
    }
    kotlinGradle {
        ktlint()
    }
}

spotbugs {
    toolVersion = "4.8.3"
    excludeFilter = file("${project.rootDir}/config/spotbugs-exclude.xml")
}
tasks.withType<SpotBugsTask>().configureEach {
    reports.create("html") {
        required = true
        setStylesheet("fancy-hist.xsl")
    }
}
