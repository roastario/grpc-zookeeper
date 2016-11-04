package com.byhiras.dist.discovery;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.byhiras.dist.discovery.ZookeeperServiceRegistrationOps;
import com.byhiras.dist.discovery.ZookeeperServiceRegistrationOps.HostandZone;

/**
 * Author stefanofranz
 */
public class ZookeeperServiceRegistrationOpsTest {

    public static final String TEST_SERVICE_ID = "testService";
    TestingServer zkTestServer;
    String host;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        host = "localhost:" + zkTestServer.getPort();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.stop();
        zkTestServer.close();
    }

    @Test
    public void shouldStoreServiceRegistration() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);

        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        assertThat(zookeeperServiceRegistrationOps1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        zookeeperServiceRegistrationOps1.close();

    }

    @Test
    public void shouldStoreServiceRegistrationTillDisconnect() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        assertThat(zookeeperServiceRegistrationOps1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        zookeeperServiceRegistrationOps1.close();

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        assertThat(zookeeperServiceRegistrationOps2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldStoreServiceRegistrationFromMultipleServicesAndMaintainAgreement() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));

        assertThat(zookeeperServiceRegistrationOps2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        assertThat(zookeeperServiceRegistrationOps1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
    }

    @Test
    public void shouldWatchForUpdates() throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        Condition signal = lock.newCondition();
        AtomicReference<List<URI>> watchedResult = new AtomicReference<>();
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);

        zookeeperServiceRegistrationOps1.watchForUpdates(TEST_SERVICE_ID, newList -> {
            watchedResult.set(newList.stream().map(HostandZone::getHostURI).collect(Collectors.toList()));
            lock.lock();
            signal.signalAll();
            lock.unlock();
        });

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps3 = new ZookeeperServiceRegistrationOps(host);
        //REGISTER SERVICE 2
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));
        //REGISTER SERVICE 3
        zookeeperServiceRegistrationOps3.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere3"));
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(asList(URI.create("dns://somewhere3"), URI.create("dns://somewhere2")))));

        //SHUTDOWN NUMBER2
        zookeeperServiceRegistrationOps2.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere3")))));

        //SHUTDOWN NUMBER3
        zookeeperServiceRegistrationOps3.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldDeregisterASingleInstanceOfAService() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        assertThat(zookeeperServiceRegistrationOps2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        zookeeperServiceRegistrationOps1.deregister(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        assertThat(zookeeperServiceRegistrationOps2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));

    }

    @Test
    public void shouldRegisterWithZoneAndRetrieveWithZone() throws Exception {

        ZookeeperServiceRegistrationOps ops1 = new ZookeeperServiceRegistrationOps(host);
        ops1.removeServiceRegistry(TEST_SERVICE_ID);
        ops1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"), "ZONE_ONE");
        ZookeeperServiceRegistrationOps ops2 = new ZookeeperServiceRegistrationOps(host);
        ops2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"), "ZONE_TWO");

        assertThat(ops2.discover(TEST_SERVICE_ID),
                is(equalTo(asList(
                        new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO"),
                        new HostandZone(URI.create("dns://somewhere1"), "ZONE_ONE")
                ))));

        ops1.deregister(TEST_SERVICE_ID, URI.create("dns://somewhere1"), "ZONE_ONE");

        assertThat(ops2.discover(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(
                new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")
        ))));
    }

    @Test
    public void shouldWatchForZonedUpdates() throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        Condition signal = lock.newCondition();
        AtomicReference<List<HostandZone>> watchedResult = new AtomicReference<>();
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);

        zookeeperServiceRegistrationOps1.watchForUpdates(TEST_SERVICE_ID, newList -> {
            watchedResult.set(newList);
            lock.lock();
            signal.signalAll();
            lock.unlock();
        });

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps3 = new ZookeeperServiceRegistrationOps(host);
        //REGISTER SERVICE 2
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"), "ZONE_TWO");
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")))));
        //REGISTER SERVICE 3
        zookeeperServiceRegistrationOps3.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere3"), "ZONE_THREE");
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(asList(
                new HostandZone(URI.create("dns://somewhere3"), "ZONE_THREE"),
                new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")
        ))));

        //SHUTDOWN NUMBER2
        zookeeperServiceRegistrationOps2.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(new HostandZone(URI.create("dns://somewhere3"), "ZONE_THREE")))));

        //SHUTDOWN NUMBER3
        zookeeperServiceRegistrationOps3.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.emptyList())));
    }

    private void waitForConditionSignal(ReentrantLock lock, Condition signal) throws InterruptedException {
        lock.lock();
        signal.await();
        lock.unlock();
    }

    @Test
    public void uriShouldBeDecomposable() throws Exception {
        URI uri = URI.create("//testService");
        System.out.println(uri.getAuthority());
    }
}