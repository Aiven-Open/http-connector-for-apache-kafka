/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package io.aiven.kafka.connect.http;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.Base58;

final class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(final KafkaContainer kafka) {
        this("5.0.4", kafka);
    }

    public SchemaRegistryContainer(final String confluentPlatformVersion, final KafkaContainer kafka) {
        super("confluentinc/cp-schema-registry:" + confluentPlatformVersion);

        dependsOn(kafka);
        withNetwork(kafka.getNetwork());
        withNetworkAliases("schema-registry-" + Base58.randomString(6));

        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                String.format("PLAINTEXT://%s:%s", kafka.getNetworkAliases().get(0), 9092));

        withExposedPorts(SCHEMA_REGISTRY_PORT);
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
    }

    public String getSchemaRegistryUrl() {
        return String.format("http://%s:%s", getContainerIpAddress(), getMappedPort(SCHEMA_REGISTRY_PORT));
    }

}
