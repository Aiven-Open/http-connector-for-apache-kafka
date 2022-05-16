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

package io.aiven.kafka.connect.http.mockserver;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public final class FailingPeriodicHandler extends AbstractHandler {

    private final List<String> recorderBodies = new CopyOnWriteArrayList<>();

    private final int requestFailPeriod;

    private int requestCounter = 0;

    public FailingPeriodicHandler(final int requestFailPeriod) {
        this.requestFailPeriod = requestFailPeriod;
    }

    @Override
    public void handle(final String target,
                       final Request baseRequest,
                       final HttpServletRequest request,
                       final HttpServletResponse response) throws IOException {
        if (requestCounter % requestFailPeriod == 0) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } else {
            recorderBodies.add(
                new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
            );
            response.setStatus(HttpServletResponse.SC_OK);
        }

        baseRequest.setHandled(true);

        requestCounter += 1;
    }

    public List<String> recorderBodies() {
        return List.copyOf(recorderBodies);
    }
}
