package io.infinivision.flink.core;

import io.infinivision.flink.athenax.backend.server.WebServer;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class DispatcherHttpServer {

    private void startHttpServer(String[] args) throws Exception {
        //启动http服务
        URI uri = UriBuilder.fromUri("http://0.0.0.0").port(8083).build();

        try (WebServer server = new WebServer(uri)) {
            server.start();
            Thread.currentThread().join();
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            new DispatcherHttpServer().startHttpServer(args);
        } catch (Exception e) {
            System.err.println("Failed to start.");
            throw e;
        }
    }
}
