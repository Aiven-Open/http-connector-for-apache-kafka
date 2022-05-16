/*
 * Copyright 2022 Aiven Oy and http-connector-for-apache-kafka project contributors
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
import java.util.AbstractMap;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import io.aiven.kafka.connect.http.mockserver.BodyRecorderHandler;
import io.aiven.kafka.connect.http.mockserver.FailingPeriodicHandler;
import io.aiven.kafka.connect.http.mockserver.MockServer;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
final class ErrantRecordTest {
    private static final Logger log = LoggerFactory.getLogger(ErrantRecordTest.class);

    private static final String HTTP_PATH = "/send-data-here";
    private static final String AUTHORIZATION = "Bearer some-token";
    private static final String CONTENT_TYPE = "application/json";

    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_TOPIC_PARTITIONS = 1;

    private static final String TEST_DLQ_TOPIC = "test-dlq-topic";
    private static final int TEST_DLQ_TOPIC_PARTITIONS = 1;
    private static final String TEST_DLQ_TOPIC_REPLICATION_FACTOR = "1";

    private static final int FAIL_PERIOD = 3;

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
    private KafkaConsumer<String, String> consumer;

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
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"
        );
        producer = new KafkaProducer<>(producerProps);

        // set up the test topic
        final List<NewTopic> testTopics = new ArrayList<NewTopic>();
        testTopics.add(new NewTopic(TEST_TOPIC, TEST_TOPIC_PARTITIONS, (short) 1));
        // and the DLQ topic
        testTopics.add(new NewTopic(TEST_DLQ_TOPIC, TEST_DLQ_TOPIC_PARTITIONS, (short) 1));
        adminClient.createTopics(testTopics).all().get();

        connectRunner = new ConnectRunner(pluginsDir, kafka.getBootstrapServers());
        connectRunner.start();
    }

    @AfterEach
    void tearDown() {
        connectRunner.stop();
        adminClient.close();
        producer.close();

        connectRunner.awaitStop();

        mockServer.stop();
    }

    @Test
    @Timeout(60)
    void testBasicDelivery() throws ExecutionException, InterruptedException {
        // Make sure enabling Errant Record support doesn't affect basic delivery
        final BodyRecorderHandler bodyRecorderHandler = new BodyRecorderHandler();
        mockServer.addHandler(bodyRecorderHandler);
        mockServer.start();

        final Map<String, String> config = errantConnectorConfig();
        connectRunner.createConnector(config);

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

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            bodyRecorderHandler.recorderBodies().size());
    }

    @Test
    @Timeout(30)
    void testFailingEvery3rdRequest() throws ExecutionException, InterruptedException {
        final FailingPeriodicHandler failingPeriodicHandler = new FailingPeriodicHandler(FAIL_PERIOD);
        mockServer.addHandler(failingPeriodicHandler);
        mockServer.start();

        final Map<String, String> config = errantConnectorConfig();
        config.put("retry.backoff.ms", "0");
        config.put("max.retries", "0");
        connectRunner.createConnector(config);

        log.info("Setting up consumer for test");

        // set up consumer on DLQ
        final Map<String, Object> consumerProps = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "kafka-connect-httpsink-test",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(TEST_DLQ_TOPIC));

        final List<String> expectedBodies = new ArrayList<>();
        final List<String> errantBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String key = "key-" + i;
                final var recordName = "user-" + i;
                final var recordValue = "value-" + i;
                final Record value = new Record(recordName, recordValue);
                if (i % FAIL_PERIOD == 0) {
                    errantBodies.add(String.format(JSON_PATTERN, recordName, recordValue));
                } else {
                    expectedBodies.add(String.format(JSON_PATTERN, recordName, recordValue));
                }
                sendFutures.add(sendMessageAsync(partition, key, value));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        await("All expected requests received by HTTP server")
                .atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100))
                .until(() -> failingPeriodicHandler.recorderBodies().size() >= expectedBodies.size());

        assertThat(failingPeriodicHandler.recorderBodies()).containsExactlyElementsOf(expectedBodies);

        log.info("{} HTTP requests were expected, {} were successfully delivered",
            expectedBodies.size(),
            failingPeriodicHandler.recorderBodies().size());

        // read from the DLQ
        final int readConsumerRetries = 100;   
        int noRecordsCount = 0;
        final List<String> dlqBodies = new ArrayList<>();

        while (noRecordsCount < readConsumerRetries) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(100));

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                continue;
            }

            for (final ConsumerRecord<String, String> record : consumerRecords) {
                // the message value is a json string and that is what we are comparing
                dlqBodies.add(record.value());
            }
            consumer.commitAsync();
        }
        consumer.close();

        // checking that all failed messages were on the DLQ
        assertThat(errantBodies).containsExactlyInAnyOrderElementsOf(dlqBodies);

        log.info("There were {} failed HTTP requests were expected, {} were found on the DLQ",
            errantBodies.size(), dlqBodies.size());
    }

    private Map<String, String> errantConnectorConfig() {
        // get around the limit of 10 entries in Map.of()
        final Map<String, String> map = Map.ofEntries(
            new AbstractMap.SimpleEntry<>("name", CONNECTOR_NAME),
            new AbstractMap.SimpleEntry<>("connector.class", HttpSinkConnector.class.getName()),
            new AbstractMap.SimpleEntry<>("topics", TEST_TOPIC),
            new AbstractMap.SimpleEntry<>("key.converter", "org.apache.kafka.connect.json.JsonConverter"),
            new AbstractMap.SimpleEntry<>("value.converter", "org.apache.kafka.connect.json.JsonConverter"),
            new AbstractMap.SimpleEntry<>("tasks.max", "1"),
            new AbstractMap.SimpleEntry<>("key.converter.schemas.enable", "false"),
            new AbstractMap.SimpleEntry<>("value.converter.schemas.enable", "false"),
            new AbstractMap.SimpleEntry<>("http.url", "http://localhost:" + mockServer.localPort() + HTTP_PATH),
            new AbstractMap.SimpleEntry<>("http.authorization.type", "static"),
            new AbstractMap.SimpleEntry<>("http.headers.authorization", AUTHORIZATION),
            new AbstractMap.SimpleEntry<>("http.headers.content.type", CONTENT_TYPE),
            new AbstractMap.SimpleEntry<>("errors.tolerance", "all"),
            new AbstractMap.SimpleEntry<>("errors.deadletterqueue.topic.name", TEST_DLQ_TOPIC),
            new AbstractMap.SimpleEntry<>("errors.deadletterqueue.context.headers.enable", "true"),
            new AbstractMap.SimpleEntry<>("errors.deadletterqueue.context.topic.replication.factor", 
                                          TEST_DLQ_TOPIC_REPLICATION_FACTOR)
            );

        return new HashMap<>(map);    
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
