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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.http.mockserver.BodyRecorderHandler;
import io.aiven.kafka.connect.http.mockserver.MockServer;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class JsonIntegrationTest {
    private static final String HTTP_PATH = "/send-data-here";
    private static final String AUTHORIZATION = "Bearer some-token";
    private static final String CONTENT_TYPE = "application/json";

    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_TOPIC_PARTITIONS = 1;

    static final String JSON_PATTERN = "{\"record\":\"%s\",\"recordValue\":\"%s\"}";

    private static File pluginsDir;

    private static final String DEFAULT_TAG = "6.0.2";

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("confluentinc/cp-kafka").withTag(DEFAULT_TAG);

    @Container
    private final KafkaContainer kafka = new KafkaContainer(DEFAULT_IMAGE_NAME)
            .withNetwork(Network.newNetwork())
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<String, Record> producer;

    private ConnectRunner connectRunner;

    private MockServer mockServer;

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        final File testDir = Files.createTempDirectory("http-connector-for-apache-kafka-").toFile();
        testDir.deleteOnExit();

        pluginsDir = new File(testDir, "plugins/");
        assert pluginsDir.mkdirs();

        // Unpack the library distribution.
        final File transformDir = new File(pluginsDir, "http-connector-for-apache-kafka/");
        assert transformDir.mkdirs();
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();
        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s",
                distFile, transformDir);
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        mockServer = new MockServer(HTTP_PATH, AUTHORIZATION, CONTENT_TYPE);

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);

        final Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"
        );
        producer = new KafkaProducer<>(producerProps);

        final NewTopic testTopic = new NewTopic(TEST_TOPIC, TEST_TOPIC_PARTITIONS, (short) 1);
        adminClient.createTopics(List.of(testTopic)).all().get();
        connectRunner = new ConnectRunner(pluginsDir, kafka.getBootstrapServers());
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();

        mockServer.stop();
    }

    @Test
    @Timeout(30)
    final void testBasicDelivery() throws ExecutionException, InterruptedException {
        final BodyRecorderHandler bodyRecorderHandler = new BodyRecorderHandler();
        mockServer.addHandler(bodyRecorderHandler);
        mockServer.start();

        connectRunner.createConnector(basicConnectorConfig());

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final var recordName = "user-" + i;
                final var recordValue = "value-" + i;
                final Record value = new Record(recordName, recordValue);
                expectedBodies.add(String.format(JSON_PATTERN, recordName, recordValue));
                sendFutures.add(sendMessageAsync(partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        await("All expected requests received by HTTP server")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> bodyRecorderHandler.recorderBodies().size() >= expectedBodies.size());

        assertThat(bodyRecorderHandler.recorderBodies()).containsExactlyElementsOf(expectedBodies);
    }

    private Map<String, String> basicConnectorConfig() {
        final var config = new HashMap<String, String>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", HttpSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC);
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("tasks.max", "1");
        config.put("key.converter.schemas.enable", "false");
        config.put("value.converter.schemas.enable", "false");
        config.put("http.url", "http://localhost:" + mockServer.localPort() + HTTP_PATH);
        config.put("http.authorization.type", "static");
        config.put("http.headers.authorization", AUTHORIZATION);
        config.put("http.headers.content.type", CONTENT_TYPE);
        return config;
    }

    private static class Record {
        @JsonProperty
        private final String record;

        @JsonProperty
        private final String recordValue;

        public Record(final String record, final String recordValue) {
            this.record = record;
            this.recordValue = recordValue;
        }
    }

    private Future<RecordMetadata> sendMessageAsync(final int partition,
                                                    final String key,
                                                    final Record value) {
        final ProducerRecord<String, Record> msg = new ProducerRecord<>(
                TEST_TOPIC, partition, key, value);
        return producer.send(msg);
    }
}
