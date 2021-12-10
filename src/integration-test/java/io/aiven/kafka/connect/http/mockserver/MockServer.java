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

package io.aiven.kafka.connect.http.mockserver;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;

public final class MockServer {
    private final Server jettyServer = new Server(0);

    private final String expectedTarget;
    private final String expectedAuthorizationHeader;
    private final String expectedContentTypeHeader;

    private final HandlerList handlers;

    public MockServer(final String expectedTarget,
               final String expectedAuthorizationHeader,
               final String expectedContentTypeHeader) {
        this.expectedTarget = expectedTarget;
        this.expectedAuthorizationHeader = expectedAuthorizationHeader;
        this.expectedContentTypeHeader = expectedContentTypeHeader;

        this.handlers = new HandlerList(
            new ExpectedAuthorizationHandler(),
            new ExpectedTargetHandler(),
            new ExpectedContentTypeHandler()
        );
        jettyServer.setHandler(this.handlers);
    }

    public MockServer(final String expectedTarget,
                      final String expectedContentTypeHeader) {
        this.expectedTarget = expectedTarget;
        this.expectedContentTypeHeader = expectedContentTypeHeader;
        this.expectedAuthorizationHeader = "";

        this.handlers = new HandlerList(
                new ExpectedTargetHandler(),
                new ExpectedContentTypeHandler()
        );

        jettyServer.setHandler(this.handlers);
    }

    public void addHandler(final Handler handler) {
        this.handlers.addHandler(handler);
    }

    public void start() {
        try {
            jettyServer.start();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int localPort() {
        return ((ServerConnector) jettyServer.getConnectors()[0]).getLocalPort();
    }

    public void stop() {
        try {
            jettyServer.stop();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final class ExpectedAuthorizationHandler extends AbstractHandler {
        @Override
        public void handle(final String target,
                           final Request baseRequest,
                           final HttpServletRequest request,
                           final HttpServletResponse response) throws IOException, ServletException {
            final String authorization = baseRequest.getHeader(HttpHeader.AUTHORIZATION.asString());
            if (!expectedAuthorizationHeader.equals(authorization)) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                baseRequest.setHandled(true);
            }
        }
    }

    private final class ExpectedTargetHandler extends AbstractHandler {
        @Override
        public void handle(final String target,
                           final Request baseRequest,
                           final HttpServletRequest request,
                           final HttpServletResponse response) throws IOException, ServletException {
            if (!expectedTarget.equals(target)) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                baseRequest.setHandled(true);
            }
        }
    }

    private final class ExpectedContentTypeHandler extends AbstractHandler {
        @Override
        public void handle(final String target,
                           final Request baseRequest,
                           final HttpServletRequest request,
                           final HttpServletResponse response) throws IOException, ServletException {
            final String contentType = baseRequest.getHeader(HttpHeader.CONTENT_TYPE.asString());
            if (!expectedContentTypeHeader.equalsIgnoreCase(contentType)) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                baseRequest.setHandled(true);
            }
        }
    }
}
