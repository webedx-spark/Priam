package org.coursera.priam.resource;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.coursera.discovery.DiscoveryTask;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/discovery")
@Produces(MediaType.APPLICATION_JSON)
public class DiscoveryResource
{
    private final DiscoveryTask discovery;

    @Inject
    public DiscoveryResource(DiscoveryTask discovery)
    {
        this.discovery = discovery;
    }

    @GET
    @Path("/advertise")
    public Response advertise() throws Exception
    {
        discovery.advertise();

        return Response.ok().build();
    }

    @GET
    @Path("/deadvertise")
    public Response deadvertise() throws Exception
    {
        discovery.deadvertise();

        return Response.ok().build();
    }

    @GET
    @Path("/info")
    public Response info() throws JSONException
    {
        JSONObject json = new JSONObject();
        json.put("nodePath", discovery.getZkNodePath());
        json.put("isAdvertising", discovery.isAdvertising());
        json.put("shouldAdvertise", discovery.shouldAdvertise());

        return Response.ok(json).build();
    }
}
