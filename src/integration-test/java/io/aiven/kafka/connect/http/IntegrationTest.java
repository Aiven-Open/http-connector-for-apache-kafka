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

package io.aiven.kafka.connect.http;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;

import io.aiven.kafka.connect.http.mockserver.BodyRecorderHandler;
import io.aiven.kafka.connect.http.mockserver.HeaderRecorderHandler;
import io.aiven.kafka.connect.http.mockserver.MockServer;
import io.aiven.kafka.connect.http.mockserver.RequestFailingHandler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
final class IntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(IntegrationTest.class);

    private static final String HTTP_PATH = "/send-data-here";
    private static final String AUTHORIZATION = "Bearer some-token";
    private static final String CONTENT_TYPE = "application/json";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_TOPIC_PARTITIONS = 1;

    private static File pluginsDir;

    @Container
    private final KafkaContainer kafka = new KafkaContainer()
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private AdminClient adminClient;
    private KafkaProducer<byte[], byte[]> producer;

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
            distFile.toString(), transformDir.toString());
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
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.ByteArraySerializer",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            "1"
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

        final Map<String, String> config = basicConnectorConfig();
        connectRunner.createConnector(config);

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;
                expectedBodies.add(value);
                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> bodyRecorderHandler.recorderBodies().size() >= expectedBodies.size(),
            15000,
            "All requests received by HTTP server"
        );
        log.info("Recorded request bodies: {}", bodyRecorderHandler.recorderBodies());
        assertIterableEquals(expectedBodies, bodyRecorderHandler.recorderBodies());

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            bodyRecorderHandler.recorderBodies().size());
    }

    @Test
    @Timeout(30)
    final void testContentTypeHeader() throws ExecutionException, InterruptedException {
        final HeaderRecorderHandler headerRecorderHandler = new HeaderRecorderHandler();
        mockServer = new MockServer(HTTP_PATH, CONTENT_TYPE);
        mockServer.addHandler(headerRecorderHandler);
        mockServer.start();

        final Map<String, String> config = basicConnectorConfig();
        config.put("http.authorization.type", "none");
        config.put("http.headers.content.type", CONTENT_TYPE);
        config.remove("http.headers.authorization");
        connectRunner.createConnector(config);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;
                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> headerRecorderHandler.recorderHeaders().size() >= 1000,
            15000,
            "All requests received by HTTP server"
        );

        headerRecorderHandler.recorderHeaders().forEach(headers -> {
            assertTrue(headers.containsKey(CONTENT_TYPE_HEADER));
            assertEquals(CONTENT_TYPE, headers.get(CONTENT_TYPE_HEADER));
        });
    }

    @Test
    @Timeout(30)
    final void testAdditionalHeaders() throws ExecutionException, InterruptedException {
        final HeaderRecorderHandler headerRecorderHandler = new HeaderRecorderHandler();
        mockServer.addHandler(headerRecorderHandler);
        mockServer.start();

        final Map<String, String> config = basicConnectorConfig();
        config.put("http.headers.additional", "test:value,test2:value2,test3:value3");
        connectRunner.createConnector(config);

        final Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("test", "value");
        expectedHeaders.put("test2", "value2");
        expectedHeaders.put("test3", "value3");
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;
                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> headerRecorderHandler.recorderHeaders().size() >= 1000,
            15000,
            "All requests received by HTTP server"
        );
        log.info("Recorded request header fields: {}", headerRecorderHandler.recorderHeaders());
        headerRecorderHandler.recorderHeaders().forEach(headers -> {
            expectedHeaders.forEach((key, value) -> {
                assertTrue(headers.containsKey(key));
                assertEquals(value, headers.get(key));
            });
        });
    }

    @Test
    @Timeout(30)
    final void testFailingEvery3rdRequest() throws ExecutionException, InterruptedException {
        mockServer.addHandler(new RequestFailingHandler(3));

        final BodyRecorderHandler bodyRecorderHandler = new BodyRecorderHandler();
        mockServer.addHandler(bodyRecorderHandler);
        mockServer.start();

        final Map<String, String> config = basicConnectorConfig();
        config.put("retry.backoff.ms", "0");
        connectRunner.createConnector(config);

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;
                expectedBodies.add(value);
                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> bodyRecorderHandler.recorderBodies().size() >= expectedBodies.size(),
            10_000,
            "All requests received by HTTP server"
        );
        log.info("Recorded request bodies: {}", bodyRecorderHandler.recorderBodies());
        assertIterableEquals(expectedBodies, bodyRecorderHandler.recorderBodies());

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            bodyRecorderHandler.recorderBodies().size());
    }

    @Test
    @Timeout(30)
    final void testAlwaysFailingHttp() throws ExecutionException, InterruptedException {
        mockServer.addHandler(new RequestFailingHandler(1));

        final BodyRecorderHandler bodyRecorderHandler = new BodyRecorderHandler();
        mockServer.addHandler(bodyRecorderHandler);
        mockServer.start();

        connectRunner.createConnector(basicConnectorConfig());

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;
                expectedBodies.add(value);
                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> {
                final ConnectorStateInfo connectorStateInfo = connectRunner.connectorState(CONNECTOR_NAME);
                assert connectorStateInfo.tasks().size() == 1;
                for (final ConnectorStateInfo.TaskState task : connectorStateInfo.tasks()) {
                    if (!task.state().equals(TaskStatus.State.FAILED.name())) {
                        return false;
                    }
                }
                return true;
            },
            10000,
            "Tasks failed"
        );
        assertIterableEquals(expectedBodies, bodyRecorderHandler.recorderBodies());
    }

    @Test
    @Timeout(30)
    final void testBatching() throws ExecutionException, InterruptedException {
        final int totalRecords = 1000;
        final int batchMaxSize = 12;

        final BodyRecorderHandler bodyRecorderHandler = new BodyRecorderHandler();
        mockServer.addHandler(bodyRecorderHandler);
        mockServer.start();

        final Map<String, String> config = basicConnectorConfig();
        config.put("batching.enabled", "true");
        config.put("batch.max.size", Integer.toString(batchMaxSize));
        connectRunner.createConnector(config);

        final List<String> expectedBodies = new ArrayList<>();
        int batchRecordCnt = 0;
        StringBuilder currentBody = new StringBuilder();

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < totalRecords; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final String value = "value-" + i;

                sendFutures.add(sendMessageAsync(TEST_TOPIC, partition, key, value));

                batchRecordCnt += 1;
                currentBody.append(value).append("\n");
                if (batchRecordCnt >= batchMaxSize) {
                    expectedBodies.add(currentBody.toString());
                    batchRecordCnt = 0;
                    currentBody = new StringBuilder();
                }
            }
        }
        if (batchRecordCnt > 0) {
            expectedBodies.add(currentBody.toString());
        }

        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TestUtils.waitForCondition(
            () -> {
                log.info("Received request bodies: {}/{}",
                    bodyRecorderHandler.recorderBodies().size(), expectedBodies.size()
                );
                return bodyRecorderHandler.recorderBodies().size() >= expectedBodies.size();
            },
            15000,
            "All requests received by HTTP server"
        );
        assertIterableEquals(expectedBodies, bodyRecorderHandler.recorderBodies());

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            bodyRecorderHandler.recorderBodies().size());
    }

    private Map<String, String> basicConnectorConfig() {
        return new HashMap<>(Map.of(
            "name", CONNECTOR_NAME,
            "connector.class", HttpSinkConnector.class.getName(),
            "topics", TEST_TOPIC,
            "key.converter", "org.apache.kafka.connect.storage.StringConverter",
            "value.converter", "org.apache.kafka.connect.storage.StringConverter",
            "tasks.max", "1",
            "http.url", "http://localhost:" + mockServer.localPort() + HTTP_PATH,
            "http.authorization.type", "static",
            "http.headers.authorization", AUTHORIZATION,
            "http.headers.content.type", CONTENT_TYPE
        ));
    }

    private Future<RecordMetadata> sendMessageAsync(final String topicName,
                                                    final int partition,
                                                    final String key,
                                                    final String value) {
        final ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(
            topicName, partition,
            key == null ? null : key.getBytes(StandardCharsets.UTF_8),
            value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        return producer.send(msg);
    }
}
