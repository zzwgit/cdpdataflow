package io.infinivision.flink.athenax.backend.server;

import io.infinivision.flink.athenax.backend.api.JobRequest;
import io.infinivision.flink.athenax.backend.api.factories.WebServiceFactory;
import io.infinivision.flink.athenax.backend.api.impl.WebServiceImpl;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@Path("/jobs")
public class WebResource {

    private final WebServiceImpl delegate = WebServiceFactory.getJobsApi();

    @GET
    @Produces({"application/json"})
    public Response listJob(@Context SecurityContext securityContext) {
        return delegate.listJob(securityContext);
    }

    @POST
    @Path("/new")
    @Produces({"application/json"})
    public Response allocateNewJob(@Context SecurityContext securityContext, JobRequest body) {
        return delegate.allocateNewJob(securityContext,body);
    }

}
