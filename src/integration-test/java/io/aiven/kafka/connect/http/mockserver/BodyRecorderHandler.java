package io.aiven.kafka.connect.http.mockserver;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public final class BodyRecorderHandler extends AbstractHandler {

    private final List<String> recorderBodies = new CopyOnWriteArrayList<>();

    @Override
    public void handle(final String target,
                       final Request baseRequest,
                       final HttpServletRequest request,
                       final HttpServletResponse response) throws IOException, ServletException {
        recorderBodies.add(
            new String(request.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
        );
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
    }

    public List<String> recorderBodies() {
        return List.copyOf(recorderBodies);
    }
}
