/*
 * Copyright 2019 Aiven Oy
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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;

final class MockServer {
    private final Server jettyServer = new Server(0);

    private final String expectedTarget;
    private final String expectedAuthorizationHeader;
    private final String expectedContentTypeHeader;

    private final List<String> recorderBodies = new CopyOnWriteArrayList<>();

    MockServer(final String expectedTarget,
               final String expectedAuthorizationHeader,
               final String expectedContentTypeHeader) {
        this.expectedTarget = expectedTarget;
        this.expectedAuthorizationHeader = expectedAuthorizationHeader;
        this.expectedContentTypeHeader = expectedContentTypeHeader;

        jettyServer.setHandler(new MockHandler());
    }

    void start() {
        try {
            jettyServer.start();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    void stop() {
        try {
            jettyServer.stop();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    int localPort() {
        return ((ServerConnector) jettyServer.getConnectors()[0]).getLocalPort();
    }

    List<String> recorderBodies() {
        return List.copyOf(recorderBodies);
    }

    private class MockHandler extends AbstractHandler {
        @Override
        public void handle(final String target,
                           final Request baseRequest,
                           final HttpServletRequest request,
                           final HttpServletResponse response) throws IOException, ServletException {
            if (!expectedTarget.equals(target)) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
                return;
            }

            final String authorization = baseRequest.getHeader(HttpHeader.AUTHORIZATION.asString());
            if (!expectedAuthorizationHeader.equals(authorization)) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                baseRequest.setHandled(true);
                return;
            }

            final String contentType = baseRequest.getHeader(HttpHeader.CONTENT_TYPE.asString());
            if (!expectedContentTypeHeader.equalsIgnoreCase(contentType)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                baseRequest.setHandled(true);
                return;
            }

            recorderBodies.add(
                new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
            );
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
        }
    }
}
