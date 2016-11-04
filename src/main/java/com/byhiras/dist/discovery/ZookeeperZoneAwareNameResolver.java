package com.byhiras.dist.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.ResolvedServerInfo;

import com.byhiras.dist.discovery.ZookeeperServiceRegistrationOps.HostandZone;
import com.google.common.base.Throwables;

/**
 * Author stefanofranz
 */
public class ZookeeperZoneAwareNameResolver extends NameResolver {

    public final String ZONE_KEY = "ZONE";


    private final URI targetUri;
    private final ZookeeperServiceRegistrationOps zookeeperServiceRegistry;
    private final Comparator<HostandZone> zoneComparator;

    public ZookeeperZoneAwareNameResolver(URI targetUri, ZookeeperServiceRegistrationOps zookeeperServiceRegistry,
                                          Comparator<HostandZone> zoneComparator) {
        this.targetUri = targetUri;
        this.zookeeperServiceRegistry = zookeeperServiceRegistry;
        this.zoneComparator = zoneComparator;
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
            List<HostandZone> initialDiscovery = zookeeperServiceRegistry.discover(serviceName);
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

    private List<List<ResolvedServerInfo>> convertToResolvedServers(List<HostandZone> newList) {
        return newList.stream().sorted(zoneComparator).map(hostandZone -> {
            try {
                URI hostURI = hostandZone.getHostURI();
                InetAddress[] allByName = InetAddress.getAllByName(hostURI.getHost());
                return Arrays.stream(allByName).map(inetAddress ->
                        new ResolvedServerInfo(new InetSocketAddress(inetAddress, hostURI.getPort()),
                                Attributes.newBuilder().set(Attributes.Key.of(ZONE_KEY), hostandZone.getZone()).build())
                ).collect(Collectors.toList());
            } catch (UnknownHostException e) {
                throw Throwables.propagate(e);
            }
        }).collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        try {
            zookeeperServiceRegistry.close();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
