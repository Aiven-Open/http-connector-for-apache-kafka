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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConnectRunner {
    private static final Logger log = LoggerFactory.getLogger(ConnectRunner.class);

    private final File pluginDir;
    private final String bootstrapServers;

    private Herder herder;
    private Connect connect;

    ConnectRunner(final File pluginDir,
                  final String bootstrapServers) {
        this.pluginDir = pluginDir;
        this.bootstrapServers = bootstrapServers;
    }

    void start() {
        final Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", bootstrapServers);

        workerProps.put("offset.flush.interval.ms", "5000");
        workerProps.put("consumer.max.poll.records", "10000"); // big enough to not interfere with batching

        // These don't matter much (each connector sets its own converters), but need to be filled with valid classes.
        workerProps.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        workerProps.put("internal.key.converter.schemas.enable", "false");
        workerProps.put("internal.value.converter.schemas.enable", "false");

        // Don't need it since we'll memory MemoryOffsetBackingStore.
        workerProps.put("offset.storage.file.filename", "");

        workerProps.put("plugin.path", pluginDir.getPath());

        final Time time = Time.SYSTEM;
        final String workerId = "test-worker";

        final Plugins plugins = new Plugins(workerProps);
        final StandaloneConfig config = new StandaloneConfig(workerProps);

        final AllConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy = 
                    new AllConnectorClientConfigOverridePolicy();

        final Worker worker = new Worker(
            workerId, time, plugins, config, new MemoryOffsetBackingStore(),
            allConnectorClientConfigOverridePolicy);
        herder = new StandaloneHerder(worker, "cluster-id", allConnectorClientConfigOverridePolicy);

        final RestServer rest = new RestServer(config);
        rest.initializeServer();

        connect = new Connect(herder, rest);

        connect.start();
    }

    void createConnector(final Map<String, String> config) throws ExecutionException, InterruptedException {
        assert herder != null;

        final FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(
            new Callback<Herder.Created<ConnectorInfo>>() {
                @Override
                public void onCompletion(final Throwable error, final Herder.Created<ConnectorInfo> info) {
                    if (error != null) {
                        log.error("Failed to create job");
                    } else {
                        log.info("Created connector {}", info.result().name());
                    }
                }
            });
        herder.putConnectorConfig(
            config.get(ConnectorConfig.NAME_CONFIG),
            config, false, cb
        );

        final Herder.Created<ConnectorInfo> connectorInfoCreated = cb.get();
        assert connectorInfoCreated.created();
    }

    ConnectorStateInfo connectorState(final String connectorName) {
        return herder.connectorStatus(connectorName);
    }

    void stop() {
        connect.stop();
    }

    void awaitStop() {
        connect.awaitStop();
    }
}
