package com.byhiras.dist.discovery;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;


import java.net.URI;
import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import com.byhiras.dist.discovery.ZookeeperServiceRegistrationOps.HostandZone;

/**
 * Author stefanofranz
 */
public class ZookeeperZoneAwareNameResolverProviderTest {


    @Test
    public void comparatorShouldPreferSameZoneHosts() throws Exception {
        Comparator<HostandZone> zoneComparator = ZookeeperZoneAwareNameResolverProvider.getZoneComparator("testZone");
        int sameZoneFirst = zoneComparator.compare(
                new HostandZone(URI.create("dns://machine1"), "testZone"),
                new HostandZone(URI.create("dns://machine1"), "notTestZone")
        );
        Assert.assertThat(sameZoneFirst, is(lessThan(0)));

        int sameZoneSecond = zoneComparator.compare(
                new HostandZone(URI.create("dns://machine1"), "notTestZone"),
                new HostandZone(URI.create("dns://machine1"), "testZone")
        );
        Assert.assertThat(sameZoneSecond, is(greaterThan(0)));

        int sameZoneAllRound = zoneComparator.compare(
                new HostandZone(URI.create("dns://machine1"), "testZone"),
                new HostandZone(URI.create("dns://machine1"), "testZone")
        );
        Assert.assertThat(sameZoneAllRound, is(0));

    }
}