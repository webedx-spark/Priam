/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.resources;

import java.io.IOException;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.ICassandraProcess;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXConnectionException;
import com.netflix.priam.utils.JMXNodeTool;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutorMBean;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import com.netflix.priam.utils.SystemUtils;

import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do general operations. Start/Stop and some JMX node tool commands
 */
@SuppressWarnings("deprecation")
@Path("/v1/cassadmin")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraAdmin
{
    private static final String REST_HEADER_KEYSPACES = "keyspaces";
    private static final String REST_HEADER_CFS = "cfnames";
    private static final String REST_HEADER_TOKEN = "token";
    private static final String REST_SUCCESS = "[\"ok\"]";
    private static final Logger logger = LoggerFactory.getLogger(CassandraAdmin.class);
    private IConfiguration config;
    private final ICassandraProcess cassProcess;

    @Inject
    public CassandraAdmin(IConfiguration config, ICassandraProcess cassProcess)
    {
        this.config = config;
        this.cassProcess = cassProcess;
    }

    @GET
    @Path("/start")
    public Response cassStart() throws IOException, InterruptedException, JSONException
    {
        cassProcess.start(true);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/stop")
    public Response cassStop() throws IOException, InterruptedException, JSONException
    {
        cassProcess.stop();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/refresh")
    public Response cassRefresh(@QueryParam(REST_HEADER_KEYSPACES) String keyspaces) throws IOException, ExecutionException, InterruptedException, JSONException
    {
        logger.debug("node tool refresh is being called");
        if (StringUtils.isBlank(keyspaces))
            return Response.status(400).entity("Missing keyspace in request").build();

        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.refresh(Lists.newArrayList(keyspaces.split(",")));
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/info")
    public Response cassInfo() throws IOException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool info being called");
        return Response.ok(nodetool.info(), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/ring/{id}")
    public Response cassRing(@PathParam("id") String keyspace) throws IOException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool ring being called");
        return Response.ok(nodetool.ring(keyspace), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/flush")
    public Response cassFlush() throws IOException, InterruptedException, ExecutionException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool flush being called");
        nodetool.flush();
        return Response.ok().build();
    }

    @GET
    @Path("/compact")
    public Response cassCompact() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool compact being called");
        nodetool.compact();
        return Response.ok().build();
    }

    @GET
    @Path("/cleanup")
    public Response cassCleanup() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool cleanup being called");
        nodetool.cleanup();
        return Response.ok().build();
    }

    @GET
    @Path("/repair")
    public Response cassRepair(@QueryParam("sequential") boolean isSequential, @QueryParam("localDC") boolean localDCOnly, @DefaultValue("false") @QueryParam("primaryRange") boolean primaryRange) throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool repair being called");
        nodetool.repair(isSequential, localDCOnly, primaryRange);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/version")
    public Response version() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        return Response.ok(new JSONArray().put(nodetool.getReleaseVersion()), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/tpstats")
    public Response tpstats() throws IOException, ExecutionException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        Iterator<Map.Entry<String, JMXEnabledThreadPoolExecutorMBean>> threads = nodetool.getThreadPoolMBeanProxies();
        JSONArray threadPoolArray = new JSONArray();
        while (threads.hasNext())
        {
            Entry<String, JMXEnabledThreadPoolExecutorMBean> thread = threads.next();
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = thread.getValue();
            JSONObject tpObj = new JSONObject();// "Pool Name", "Active",
                                                // "Pending", "Completed",
                                                // "Blocked", "All time blocked"
            tpObj.put("pool name", thread.getKey());
            tpObj.put("active", threadPoolProxy.getActiveCount());
            tpObj.put("pending", threadPoolProxy.getPendingTasks());
            tpObj.put("completed", threadPoolProxy.getCompletedTasks());
            tpObj.put("blocked", threadPoolProxy.getCurrentlyBlockedTasks());
            tpObj.put("total blocked", threadPoolProxy.getTotalBlockedTasks());
            threadPoolArray.put(tpObj);
        }
        JSONObject droppedMsgs = new JSONObject();
        for (Entry<String, Integer> entry : nodetool.getDroppedMessages().entrySet())
            droppedMsgs.put(entry.getKey(), entry.getValue());

        JSONObject rootObj = new JSONObject();
        rootObj.put("thread pool", threadPoolArray);
        rootObj.put("dropped messages", droppedMsgs);

        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/compactionstats")
    public Response compactionStats() throws IOException, ExecutionException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        JSONObject rootObj = new JSONObject();
        CompactionManagerMBean cm = nodetool.getCompactionManagerProxy();
        rootObj.put("pending tasks", cm.getPendingTasks());
        JSONArray compStats = new JSONArray();
        for (Map<String, String> c : cm.getCompactions())
        {
            JSONObject cObj = new JSONObject();
            cObj.put("id", c.get("id"));
            cObj.put("keyspace", c.get("keyspace"));
            cObj.put("columnfamily", c.get("columnfamily"));
            cObj.put("bytesComplete", c.get("bytesComplete"));
            cObj.put("totalBytes", c.get("totalBytes"));
            cObj.put("taskType", c.get("taskType"));
            String percentComplete = new Long(c.get("totalBytes")) == 0 ? "n/a" : new DecimalFormat("0.00").format((double) new Long(c.get("bytesComplete")) / new Long(c.get("totalBytes")) * 100) + "%";
            cObj.put("progress", percentComplete);
            compStats.put(cObj);
        }
        rootObj.put("compaction stats", compStats);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/disablegossip")
    public Response disablegossip() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.stopGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/enablegossip")
    public Response enablegossip() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.startGossiping();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/disablethrift")
    public Response disablethrift() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.stopThriftServer();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/enablethrift")
    public Response enablethrift() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.startThriftServer();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/statusthrift")
    public Response statusthrift() throws IOException, ExecutionException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        return Response.ok(new JSONObject().put("status", (nodetool.isThriftServerRunning() ? "running" : "not running")), MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/gossipinfo")
    public Response gossipinfo() throws IOException, ExecutionException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        JSONObject rootObj = new JSONObject();
        String[] ginfo = nodetool.getGossipInfo().split("/");
        for (String info : ginfo)
        {
            String[] data = info.split("\n");
            String key = "";
            JSONObject obj = new JSONObject();
            for (String element : data)
            {
                String[] kv = element.split(":");
                if (kv.length == 1)
                    key = kv[0];
                else
                    obj.put(kv[0], kv[1]);
            }
            if (StringUtils.isNotBlank(key))
                rootObj.put(key, obj);
        }
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/netstats")
    public Response netstats(@QueryParam("host") String hostname) throws IOException, ExecutionException, InterruptedException, JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        JSONObject rootObj = new JSONObject();
        rootObj.put("mode", nodetool.getOperationMode());
        final InetAddress addr = (hostname == null) ? null : InetAddress.getByName(hostname);

        /*
        Map<InetAddress, List<String>> hostSendFiles = new HashMap<InetAddress, List<String>>();
        Map<InetAddress, List<String>> hostRecvFiles = new HashMap<InetAddress, List<String>>();

        for(StreamState status : nodetool.getStreamStatus())
        {
        	for (SessionInfo info : status.sessions)
        	{
        		if (!info.receivingSummaries.isEmpty())
        		{
        			for (ProgressInfo progress : info.getReceivingFiles())
        			{
        				if (!hostRecvFiles.containsKey(progress.peer))
        					hostRecvFiles.put(progress.peer, new ArrayList<String>());
        				hostRecvFiles.get(progress.peer).add(progress.fileName);
        			}
        		}
        		if (!info.sendingSummaries.isEmpty())
        		{
        			for (ProgressInfo progress : info.getSendingFiles())
        			{
        				if (!hostSendFiles.containsKey(progress.peer))
        					hostSendFiles.put(progress.peer, new ArrayList<String>());
        				hostSendFiles.get(progress.peer).add(progress.fileName);
        			}
        		}
        	}
        } */

        JSONObject hostSendFiles = new JSONObject();
        JSONObject hostRecvFiles = new JSONObject();

        for(StreamState status : nodetool.getStreamStatus())
        {
        	for (SessionInfo info : status.sessions)
        	{
        		if (!info.receivingSummaries.isEmpty())
        		{
        			for (ProgressInfo progress : info.getReceivingFiles())
        			{
        				hostRecvFiles.append(progress.peer.getHostAddress(), progress.fileName);
        			}
        		}
        		if (!info.sendingSummaries.isEmpty())
        		{
        			for (ProgressInfo progress : info.getSendingFiles())
        			{
        				hostSendFiles.append(progress.peer.getHostAddress(), progress.fileName);
        			}
        		}
        	}
        }

        if (hostSendFiles.length() == 0)
        	rootObj.put("Sending", "Not sending any streams.");
        rootObj.put("hosts sending", hostSendFiles);

        if (hostRecvFiles.length() == 0)
            rootObj.put("receiving", "Not receiving any streams.");
        rootObj.put("hosts receiving", hostRecvFiles);

        MessagingServiceMBean ms = nodetool.msProxy;
        int pending;
        long completed;
        pending = 0;
        for (int n : ms.getCommandPendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getCommandCompletedTasks().values())
            completed += n;
        JSONObject cObj = new JSONObject();
        cObj.put("active", "n/a");
        cObj.put("pending", pending);
        cObj.put("completed", completed);
        rootObj.put("commands", cObj);

        pending = 0;
        for (int n : ms.getResponsePendingTasks().values())
            pending += n;
        completed = 0;
        for (long n : ms.getResponseCompletedTasks().values())
            completed += n;
        JSONObject rObj = new JSONObject();
        rObj.put("active", "n/a");
        rObj.put("pending", pending);
        rObj.put("completed", completed);
        rootObj.put("responses", rObj);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/move")
    public Response moveToken(@QueryParam(REST_HEADER_TOKEN) String newToken) throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        nodetool.move(newToken);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/scrub")
    public Response scrub(@QueryParam(REST_HEADER_KEYSPACES) String keyspaces, @QueryParam(REST_HEADER_CFS) String cfnames, @QueryParam("noSnapshot") @DefaultValue("false") Boolean noSnapshot, @QueryParam("skipCorrupted") @DefaultValue("false") Boolean skipCorrupted) throws IOException, ExecutionException, InterruptedException,
            ConfigurationException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        String[] cfs = null;
        if (StringUtils.isNotBlank(cfnames))
            cfs = cfnames.split(",");
        if (cfs == null)
            nodetool.scrub(noSnapshot, skipCorrupted, keyspaces);
        else
            nodetool.scrub(noSnapshot, skipCorrupted, keyspaces, cfs);
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/cfhistograms")
    public Response cfhistograms(@QueryParam(REST_HEADER_KEYSPACES) String keyspace, @QueryParam(REST_HEADER_CFS) String cfname) throws IOException, ExecutionException, InterruptedException,
            JSONException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        if (StringUtils.isBlank(keyspace) || StringUtils.isBlank(cfname))
            return Response.status(400).entity("Missing keyspace/cfname in request").build();

        ColumnFamilyStoreMBean store = nodetool.getCfsProxy(keyspace, cfname);

        // default is 90 offsets
        long[] offsets = new EstimatedHistogram().getBucketOffsets();

        long[] rrlh = store.getRecentReadLatencyHistogramMicros();
        long[] rwlh = store.getRecentWriteLatencyHistogramMicros();
        long[] sprh = store.getRecentSSTablesPerReadHistogram();
        long[] ersh = store.getEstimatedRowSizeHistogram();
        long[] ecch = store.getEstimatedColumnCountHistogram();

        JSONObject rootObj = new JSONObject();
        JSONArray columns = new JSONArray();
        columns.put("offset");
        columns.put("sstables");
        columns.put("write latency");
        columns.put("read latency");
        columns.put("row size");
        columns.put("column count");
        rootObj.put("columns", columns);
        JSONArray values = new JSONArray();
        for (int i = 0; i < offsets.length; i++)
        {
            JSONArray row = new JSONArray();
            row.put(offsets[i]);
            row.put(i < sprh.length ? sprh[i] : "");
            row.put(i < rwlh.length ? rwlh[i] : "");
            row.put(i < rrlh.length ? rrlh[i] : "");
            row.put(i < ersh.length ? ersh[i] : "");
            row.put(i < ecch.length ? ecch[i] : "");
            values.put(row);
        }
        rootObj.put("values", values);
        return Response.ok(rootObj, MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/drain")
    public Response cassDrain() throws IOException, ExecutionException, InterruptedException
    {
        JMXNodeTool nodetool = null;
		try {
			nodetool = JMXNodeTool.instance(config);
		} catch (JMXConnectionException e) {
			return Response.status(503).entity("JMXConnectionException")
					.build();
		}
        logger.debug("node tool drain being called");
        nodetool.drain();
        return Response.ok(REST_SUCCESS, MediaType.APPLICATION_JSON).build();
    }
}
