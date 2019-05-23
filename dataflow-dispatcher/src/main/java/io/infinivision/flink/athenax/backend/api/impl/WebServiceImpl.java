/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.infinivision.flink.athenax.backend.api.impl;

import io.infinivision.flink.athenax.backend.api.JobRequest;
import io.infinivision.flink.core.DispatcherConsoleServer;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;

public class WebServiceImpl {

    public Response allocateNewJob(SecurityContext securityContext, JobRequest body) {

        ArrayList<String> ops = buildCommands(body);

        DispatcherConsoleServer dispatcher = new DispatcherConsoleServer();
        try {
            String result = dispatcher.dispatching(ops.toArray(new String[ops.size()]));
            return Response.ok().entity(result).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.ok().entity(e).build();
        }

    }

    public Response getJob(SecurityContext securityContext, String jobId) {
        // do some magic!
        return Response.ok().entity("magic!").build();
    }

    public Response listJob(SecurityContext securityContext) {
        // do some magic!
        return Response.ok().entity("magic!").build();
    }

    public Response updateJob(SecurityContext securityContext, String jobId) {
        // do some magic!
        return Response.ok().entity("magic!").build();
    }

    private ArrayList<String> buildCommands(JobRequest body) {
        ArrayList<String> ops = new ArrayList<>();
        ops.add("run");
        ops.add("-s");
        ops.add(body.getSession());
        ops.add("-sqlPath");
        ops.add(body.getSqlPath());
        for (String jar : body.getJar()) {
            ops.add("-j");
            ops.add(jar);
        }
        for (String lib : body.getLibrary()) {
            ops.add("-l");
            ops.add(lib);
        }
        return ops;
    }
}

