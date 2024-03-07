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

package io.aiven.kafka.connect.http.metrics;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;

import io.aiven.kafka.connect.http.HttpSinkConnector;
import io.aiven.kafka.connect.http.config.HttpSinkConfig;

public class HttpConnectorMetrics {
    private static final String SOURCE_CONNECTOR_GROUP = HttpSinkConnector.class.getSimpleName();

    private volatile AtomicInteger retryCount = new AtomicInteger(0);

    public HttpConnectorMetrics(final HttpSinkConfig config) {
        final Metrics metrics = new Metrics();
        metrics.addMetric(metrics.metricName("retry-count", SOURCE_CONNECTOR_GROUP,
                        "The number of completed retries made in the current task."),
            (metricConfig, now) -> getRetryCount());
        final JmxReporter reporter = new JmxReporter();
        reporter.contextChange(() -> Map.of(MetricsContext.NAMESPACE, "kafka.connect.http-sink"));
        metrics.addReporter(reporter);
    }

    protected int getRetryCount() {
        return retryCount.get();
    }

    public void incrementRetryCount() {
        retryCount.incrementAndGet();
    }

    public void resetRetryCount() {
        retryCount.set(0);
    }
}
