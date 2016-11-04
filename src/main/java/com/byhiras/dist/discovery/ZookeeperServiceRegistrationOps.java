package com.byhiras.dist.discovery;

import java.beans.ConstructorProperties;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Author stefanofranz
 */
public class ZookeeperServiceRegistrationOps implements Closeable {

    private static final String ZK_ROOT = "/services";
    private static final String ZK_DELIMETER = "/";
    protected final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperServiceRegistrationOps.class);
    public static final String ZONE_DELIMITER_REGEX = "\\|\\*\\*\\|";
    public static final String ZONE_DELIMITER = "|**|";

    private CuratorFramework curatorFramework;

    private final static String UNKNOWN_ZONE = "UNKN";


    @ConstructorProperties({"address"})
    public ZookeeperServiceRegistrationOps(final String address) {
        curatorFramework = CuratorFrameworkFactory.newClient(address, new ExponentialBackoffRetry(1000, 5));
        curatorFramework.start();
    }

    public void registerService(final String serviceId, final URI endpointURI, final String zone) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        curatorFramework
                .create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(znode + ZK_DELIMETER, (endpointURI.toASCIIString()+ ZONE_DELIMITER +zone).getBytes());
    }

    public void registerService(final String serviceId, final URI endpointURI) throws Exception {
        registerService(serviceId, endpointURI, UNKNOWN_ZONE);
    }

    private String ensureNodeForServiceExists(String serviceId) throws Exception {
        String znode = ZK_ROOT + ZK_DELIMETER + serviceId;
        return ensureNodeExists(znode);
    }

    private String ensureNodeExists(String znode) throws Exception {
        if (curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(znode) == null) {
            try {
                curatorFramework.create().creatingParentsIfNeeded().forPath(znode);
            } catch (KeeperException.NodeExistsException e) {
                //Another Thread/Service/Machine has just created this node for us.
            }
        }
        return znode;
    }

    public List<HostandZone> discover(final String serviceId) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        return getUrisForServiceNode(znode);
    }

    public List<URI> discoverUnzoned(final String serviceId) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        return getUrisForServiceNode(znode).stream().map(HostandZone::getHostURI).collect(Collectors.toList());
    }

    private List<HostandZone> getUrisForServiceNode(String znode) throws Exception {
        return getServicesForNode(znode).stream().map(storedString -> {
            String[] split = storedString.split(ZONE_DELIMITER_REGEX);
            return new HostandZone(URI.create(split[0]), split[1]);
        }).collect(Collectors.toList());
    }

    private List<String> getServicesForNode(String znode) throws Exception {
        ensureNodeExists(znode);
        List<String> children = curatorFramework.getChildren().forPath(znode);
        return children.stream().map(child -> {
            try {
                return curatorFramework.getData().forPath(znode + ZK_DELIMETER + child);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }).map(String::new).collect(Collectors.toList());
    }

    public boolean deregister(final String serviceId, final URI uriToDeregister, final String zone) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        List<String> children = curatorFramework.getChildren().forPath(znode);
        children.stream().forEach(child -> {
            try {
                String storedUri = new String(curatorFramework.getData().forPath(znode + ZK_DELIMETER + child));
                if (storedUri.equals(uriToDeregister.toASCIIString()+ZONE_DELIMITER+zone)) {
                    curatorFramework.delete().forPath(znode + ZK_DELIMETER + child);
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
        return true;
    }

    public boolean deregister(final String serviceId, final URI uriToDeregister) throws Exception {
        return deregister(serviceId, uriToDeregister, UNKNOWN_ZONE);
    }

    public boolean watchForUpdates(final String serviceId, ServiceStateListener listener) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        return watchNodeForUpdates(znode, listener);
    }

    private boolean watchNodeForUpdates(final String node, ServiceStateListener listener) throws Exception {
        try {
            String znode = ensureNodeExists(node);
            curatorFramework.getChildren().usingWatcher((Watcher) watchedEvent -> {
                try {
                    watchNodeForUpdates(znode, listener);
                    listener.update(getUrisForServiceNode(watchedEvent.getPath()));
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }).forPath(znode);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean removeServiceRegistry(String serviceId) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        try {
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(znode);
            return true;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public interface ServiceStateListener {
        void update(List<HostandZone> newList);
    }

    @Override
    public void close() throws IOException {
        curatorFramework.close();
    }

    public static class HostandZone{
        private final URI hostURI;
        private final String zone;

        public HostandZone(URI hostURI, String zone) {
            this.hostURI = hostURI;
            this.zone = zone;
        }

        public String getZone() {
            return zone;
        }

        public URI getHostURI() {
            return hostURI;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HostandZone that = (HostandZone) o;
            return Objects.equals(hostURI, that.hostURI) &&
                    Objects.equals(zone, that.zone);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zone, hostURI);
        }

        @Override
        public String toString() {
            return "HostandZone{" +
                    "hostURI=" + hostURI +
                    ", zone='" + zone + '\'' +
                    '}';
        }
    }
}
