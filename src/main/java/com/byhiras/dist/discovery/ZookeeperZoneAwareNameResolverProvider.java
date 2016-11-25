package com.byhiras.dist.discovery;

import java.net.URI;
import java.util.Comparator;
import javax.annotation.Nullable;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

import com.google.common.annotations.VisibleForTesting;

/**
 * Author stefanofranz
 */
public class ZookeeperZoneAwareNameResolverProvider extends NameResolverProvider {

    private static final String SCHEME = "zk";
    private final String zookeeperAddress;
    private final Comparator<ZookeeperServiceRegistrationOps.HostandZone> hostComparator;

    private ZookeeperZoneAwareNameResolverProvider(String zookeeperAddress, Comparator<ZookeeperServiceRegistrationOps.HostandZone> hostComparator) {
        this.zookeeperAddress = zookeeperAddress;
        this.hostComparator = hostComparator;
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
        return new ZookeeperZoneAwareNameResolver(targetUri, new ZookeeperServiceRegistrationOps(zookeeperAddress), hostComparator);
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @VisibleForTesting
    static Comparator<ZookeeperServiceRegistrationOps.HostandZone> getZoneComparator(final String zoneToPrefer) {
        return (o1, o2) -> {
            if (zoneToPrefer.equals(o1.getZone()) && zoneToPrefer.equals(o2.getZone())) {
                return o1.getHostURI().compareTo(o2.getHostURI());
            } else if (zoneToPrefer.equals(o1.getZone()) && !zoneToPrefer.equals(o2.getZone())) {
                return -1;
            } else if (!zoneToPrefer.equals(o1.getZone()) && zoneToPrefer.equals(o2.getZone())) {
                return 1;
            } else {
                return o1.getHostURI().compareTo(o2.getHostURI());
            }
        };
    }

    public static class Builder {
        private String zookeeperAddress;
        private String zoneToPrefer;

        public Builder setZookeeperAddress(String zookeeperAddress) {
            this.zookeeperAddress = zookeeperAddress;
            return this;
        }

        public Builder setPreferredZone(String zoneToPrefer) {
            this.zoneToPrefer = zoneToPrefer;
            return this;
        }


        public NameResolverProvider build() {
            Comparator<ZookeeperServiceRegistrationOps.HostandZone> comparator;
            if (zoneToPrefer != null) {
                comparator = getZoneComparator(zoneToPrefer);
            } else {
                comparator = Comparator.comparing(hostandZone -> hostandZone.getHostURI().getHost(), Comparator.naturalOrder());
            }
            return new ZookeeperZoneAwareNameResolverProvider(zookeeperAddress, comparator);
        }
    }
}
