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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import io.aiven.kafka.connect.http.config.HttpSinkConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkConnector.class);

    private Map<String, String> configProps;
    private HttpSinkConfig config;

    // required by Connect
    public HttpSinkConnector() {
    }

    @Override
    public ConfigDef config() {
        return HttpSinkConfig.configDef();
    }

    @Override
    public void start(final Map<String, String> props) {
        Objects.requireNonNull(props);

        this.configProps = Collections.unmodifiableMap(props);
        this.config = new HttpSinkConfig(props);
        log.info("Starting connector {}", config.connectorName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.nCopies(maxTasks, Map.copyOf(configProps));
    }

    @Override
    public void stop() {
        // Nothing to do.
        log.info("Stopping connector {}", config.connectorName());
    }

    @Override
    public String version() {
        return Version.VERSION;
    }
}
