package org.coursera.discovery;

import com.google.common.base.Charsets;
import com.netflix.priam.IConfigSource;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.CassandraMonitor;
import com.netflix.priam.utils.JMXNodeTool;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class DiscoveryTask extends Task implements ConnectionStateListener
{
    public static final String JOBNAME = "DISCOVERY_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryTask.class);

    private final CuratorFramework curatorFramework;
    private String zkList;

    private final CassandraMonitor cassandraMonitor;

    private final String serviceName;
    private final int port;
    private final AtomicBoolean shouldAdvertise = new AtomicBoolean(true);

    private String nodePath;
    private AtomicBoolean isAdvertising = new AtomicBoolean(false);

    @Inject
    public DiscoveryTask(IConfiguration config, CassandraMonitor cassandraMonitor)
    {
        super(config);

        this.cassandraMonitor = cassandraMonitor;

        serviceName = config.getAppName() + config.getDCSuffix();

        if (config.getDCSuffix().startsWith("_solr")) {
            port = 8983;
        } else {
            port = 9142;
        }

        zkList = config.getZkServers();

        logger.info("Connecting to zookeepers {}.", zkList);

        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkList)
                .retryPolicy(new ExponentialBackoffRetry(150, 6))
                .namespace("services")
                .connectionTimeoutMs(20000)
                .build();

        curatorFramework.start();

        curatorFramework.getConnectionStateListenable().addListener(this);
    }

    @Override
    public void execute() throws Exception
    {
        if (isCassandraRunning() && shouldAdvertise.get())
            advertiseIfNeeded();
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public void advertise() throws Exception
    {
        shouldAdvertise.set(true);
        advertiseIfNeeded();
    }

    public void deadvertise() throws Exception
    {
        shouldAdvertise.set(false);
        deadvertiseIfNeeded();
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 30 * 1000L);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        if (newState.equals(ConnectionState.LOST)) {
            logger.warn("Lost zookeeper connection");
            isAdvertising.set(false);
        } else if (newState.equals(ConnectionState.RECONNECTED)) {
            logger.info("Required zookeeper connection, readvertising");
            isAdvertising.set(true);
        }
    }

    private synchronized void advertiseIfNeeded() throws Exception
    {
        if (isAdvertising.get())
            return;

        if (nodePath == null) {
            String path = ZKPaths.makePath(serviceName, "node");
            nodePath = curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withProtection()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, getZkData());
        } else {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(nodePath, getZkData());
        }

        logger.info("Successfully advertised with path {}.", nodePath);

        isAdvertising.set(true);
    }

    private synchronized void deadvertiseIfNeeded() throws Exception
    {
        if (!isAdvertising.get())
            return;

        curatorFramework.delete().forPath(nodePath);

        isAdvertising.set(false);
    }

    private byte[] getZkData() throws JSONException
    {
        JSONObject data = new JSONObject();

        data.put("address", config.getHostIP());
        data.put("port", port);
        data.put("zone", config.getRac());
        data.put("asgName", config.getASGName());
        data.put("instanceId", config.getInstanceName());

        return data.toString().getBytes(Charsets.UTF_8);
    }

    private boolean isCassandraRunning()
    {
        try
        {
            JMXNodeTool tool = JMXNodeTool.instance(config);
            return tool.getOperationalMode().equals("NORMAL");
        }
        catch (Exception e)
        {
            logger.debug("Exception while determining node status.", e);
            return false;
        }
    }
}
