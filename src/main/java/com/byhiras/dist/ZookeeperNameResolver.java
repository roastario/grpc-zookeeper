package com.byhiras.dist;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.ResolvedServerInfo;

import com.google.common.base.Throwables;

/**
 * Author stefanofranz
 */
public class ZookeeperNameResolver extends NameResolver {


    private final URI targetUri;
    private final ZookeeperServiceRegistrationOps zookeeperServiceRegistry;

    public ZookeeperNameResolver(URI targetUri, ZookeeperServiceRegistrationOps zookeeperServiceRegistry) {
        this.targetUri = targetUri;
        this.zookeeperServiceRegistry = zookeeperServiceRegistry;
    }


    @Override
    public String getServiceAuthority() {
        return targetUri.getAuthority();
    }

    @Override
    public void start(Listener listener) {

        //FORMAT WILL BE: zk://serviceName
        String serviceName = targetUri.getAuthority();

        try {
            List<URI> initialDiscovery = zookeeperServiceRegistry.discover(serviceName);
            List<List<ResolvedServerInfo>> initialServers = convertToResolvedServers(initialDiscovery);
            listener.onUpdate(initialServers, Attributes.EMPTY);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            zookeeperServiceRegistry.watchForUpdates(serviceName, updatedList -> {
                List<List<ResolvedServerInfo>> resolvedServers = convertToResolvedServers(updatedList);
                listener.onUpdate(resolvedServers, Attributes.EMPTY);
            });
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    private List<List<ResolvedServerInfo>> convertToResolvedServers(List<URI> newList) {
        return newList.stream().map(uri -> {
            try {
                InetAddress[] allByName = InetAddress.getAllByName(uri.getHost());
                return Arrays.stream(allByName).map(inetAddress ->
                        new ResolvedServerInfo(new InetSocketAddress(inetAddress, uri.getPort()), Attributes.EMPTY)
                ).collect(Collectors.toList());
            } catch (UnknownHostException e) {
                throw Throwables.propagate(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public void shutdown() {

    }

    public static class ZookeeperNameResolverProvider extends NameResolverProvider {

        private static final String SCHEME = "zk";
        private final String zookeeperAddress;

        public ZookeeperNameResolverProvider(String zookeeperAddress) {
            this.zookeeperAddress = zookeeperAddress;
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            return 5;
        }

        @Nullable
        @Override
        public NameResolver newNameResolver(URI targetUri, Attributes params) {
            return new ZookeeperNameResolver(targetUri, new ZookeeperServiceRegistrationOps(zookeeperAddress));
        }

        @Override
        public String getDefaultScheme() {
            return SCHEME;
        }
    }
}
