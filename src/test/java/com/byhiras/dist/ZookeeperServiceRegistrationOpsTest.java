package com.byhiras.dist;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    }

    @Test
    public void shouldStoreServiceRegistration() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);

        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        Assert.assertThat(zookeeperServiceRegistrationOps1.discover(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        zookeeperServiceRegistrationOps1.close();

    }

    @Test
    public void shouldStoreServiceRegistrationTillDisconnect() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        Assert.assertThat(zookeeperServiceRegistrationOps1.discover(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        zookeeperServiceRegistrationOps1.close();

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        Assert.assertThat(zookeeperServiceRegistrationOps2.discover(TEST_SERVICE_ID), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldStoreServiceRegistrationFromMultipleServicesAndMaintainAgreement() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));

        Assert.assertThat(zookeeperServiceRegistrationOps2.discover(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        Assert.assertThat(zookeeperServiceRegistrationOps1.discover(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
    }

    @Test
    public void shouldWatchForUpdates() throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        Condition signal = lock.newCondition();
        AtomicReference<List<URI>> watchedResult = new AtomicReference<>();
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
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        waitForConditionSignal(lock, signal);
        Assert.assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));
        //REGISTER SERVICE 3
        zookeeperServiceRegistrationOps3.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere3"));
        waitForConditionSignal(lock, signal);
        Assert.assertThat(watchedResult.get(), is(equalTo(asList(URI.create("dns://somewhere3"), URI.create("dns://somewhere2")))));

        //SHUTDOWN NUMBER2
        zookeeperServiceRegistrationOps2.close();
        waitForConditionSignal(lock, signal);
        Assert.assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere3")))));

        //SHUTDOWN NUMBER3
        zookeeperServiceRegistrationOps3.close();
        waitForConditionSignal(lock, signal);
        Assert.assertThat(watchedResult.get(), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldDeregisterASingleInstanceOfAService() throws Exception {

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps1 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps1.removeServiceRegistry(TEST_SERVICE_ID);
        zookeeperServiceRegistrationOps1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps2 = new ZookeeperServiceRegistrationOps(host);
        zookeeperServiceRegistrationOps2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        Assert.assertThat(zookeeperServiceRegistrationOps2.discover(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        zookeeperServiceRegistrationOps1.deregister(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        Assert.assertThat(zookeeperServiceRegistrationOps2.discover(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));

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