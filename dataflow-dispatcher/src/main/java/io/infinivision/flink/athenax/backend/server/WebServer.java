package io.infinivision.flink.athenax.backend.server;

import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;

import java.io.IOException;
import java.net.URI;
import java.util.StringJoiner;

public class WebServer implements AutoCloseable {
  public static final String BASE_PATH = "/ws/v1";

  private static final String[] PACKAGES = new String[] {
          "io.infinivision.flink.athenax.backend.server",
          "com.fasterxml.jackson.jaxrs.json"
  };

  private final HttpServer server;

  public WebServer(URI endpoint) throws IOException {
    this.server = GrizzlyServerFactory.createHttpServer(endpoint, new HttpHandler() {

      @Override
      public void service(Request rqst, Response rspns) throws Exception {
        rspns.setStatus(HttpStatus.NOT_FOUND_404.getStatusCode(), "Not found");
        rspns.getWriter().write("404: not found");
      }
    });

    WebappContext context = new WebappContext("WebappContext", BASE_PATH);
    ServletRegistration registration = context.addServlet("ServletContainer", ServletContainer.class);
    registration.setInitParameter(ServletContainer.RESOURCE_CONFIG_CLASS,
            PackagesResourceConfig.class.getName());

    StringJoiner sj = new StringJoiner(",");
    for (String s : PACKAGES) {
      sj.add(s);
    }

    registration.setInitParameter(PackagesResourceConfig.PROPERTY_PACKAGES, sj.toString());
    registration.addMapping(BASE_PATH);
    context.deploy(server);
  }

  public void start() throws IOException {
    server.start();
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }

  public int port() {
    return server.getListeners().iterator().next().getPort();
  }
}
