package com.byhiras.dist;

import java.beans.ConstructorProperties;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
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

    private CuratorFramework curatorFramework;


    @ConstructorProperties({"address"})
    public ZookeeperServiceRegistrationOps(final String address) {
        curatorFramework = CuratorFrameworkFactory.newClient(address, new ExponentialBackoffRetry(1000, 5));
        curatorFramework.start();
    }

    protected void registerService(final String serviceId, final URI endpointURI) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        curatorFramework
                .create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(znode + ZK_DELIMETER, endpointURI.toASCIIString().getBytes());

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

    protected List<URI> discover(final String serviceId) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        return getUrisForServiceNode(znode);
    }

    private List<URI> getUrisForServiceNode(String znode) throws Exception {
        return getServicesForNode(znode).stream().map(URI::create).collect(Collectors.toList());
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

    protected boolean deregister(final String serviceId, final URI uriToDeregister) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        List<String> children = curatorFramework.getChildren().forPath(znode);
        children.stream().forEach(child -> {
            try {
                String storedUri = new String(curatorFramework.getData().forPath(znode + ZK_DELIMETER + child));
                if (storedUri.equals(uriToDeregister.toASCIIString())) {
                    curatorFramework.delete().forPath(znode + ZK_DELIMETER + child);
                }
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
        return true;
    }

    protected boolean watchForUpdates(final String serviceId, ServiceStateListener listener) throws Exception {
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

    protected boolean removeServiceRegistry(String serviceId) throws Exception {
        String znode = ensureNodeForServiceExists(serviceId);
        try {
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(znode);
            return true;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public interface ServiceStateListener {
        void update(List<URI> newList);
    }

    @Override
    public void close() throws IOException {
        curatorFramework.close();
    }
}
