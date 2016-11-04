package com.byhiras.dist.discovery;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import com.byhiras.dist.healthchecking.PingPongServiceImpl;
import com.byhiras.dist.common.Health;
import com.byhiras.dist.common.PingPongGrpc;
import com.byhiras.dist.loadbalancing.HealthAwareLoadBalancerFactory;

/**
 * Author stefanofranz
 */
public class ZookeeperZoneAwareNameResolverTest {

    TestingServer zkTestServer;
    String zkHost;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        zkHost = "localhost:" + zkTestServer.getPort();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.stop();
        zkTestServer.close();
    }

    @Test
    public void shouldResolveSingleAddress() throws Exception {
        Server server = ServerBuilder.forPort(0).addService(new PingPongServiceImpl() {
        }).build();
        server.start();
        int serverPort = server.getPort();
        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps = new ZookeeperServiceRegistrationOps(zkHost);
        zookeeperServiceRegistrationOps.removeServiceRegistry("pingPongService");
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + serverPort));
        ManagedChannel channel = ManagedChannelBuilder.forTarget("zk://pingPongService")
                .nameResolverFactory(
                        ZookeeperZoneAwareNameResolverProvider.newBuilder()
                                .setZookeeperAddress(zkHost).build()
                )
                .usePlaintext(true).build();
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
        Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
        Assert.assertThat(pong, is(notNullValue()));
    }

    @Test
    public void shouldResolveMultipleAddressesAndAllowLoadBalancing() throws Exception {

        AtomicInteger server1Count = new AtomicInteger();
        AtomicInteger server2Count = new AtomicInteger();

        Server server1 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                server1Count.incrementAndGet();
                responseObserver.onNext(Health.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server1.start();

        int server1Port = server1.getPort();

        Server server2 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                server2Count.incrementAndGet();
                responseObserver.onNext(Health.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server2.start();

        int server2Port = server2.getPort();

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps = new ZookeeperServiceRegistrationOps(zkHost);
        zookeeperServiceRegistrationOps.removeServiceRegistry("pingPongService");
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server1Port));
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server2Port));

        ManagedChannel channel = ManagedChannelBuilder.forTarget("zk://pingPongService").nameResolverFactory(
                ZookeeperZoneAwareNameResolverProvider.newBuilder().setZookeeperAddress(zkHost).build())
                .usePlaintext(true)
                .loadBalancerFactory(HealthAwareLoadBalancerFactory.withRoundRobinOnly())
                .build();
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);

        for (int i = 0; i < 100; i++) {
            Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
            Assert.assertThat(pong, is(notNullValue()));
        }

        double weightedDifference = Math.abs(server1Count.get() - server2Count.get()) / (double) (server1Count.get() + server2Count.get());
        //check that the loadbalancer is not too skewed (less than 20% difference between the two server counts)
        Assert.assertThat(weightedDifference, is(lessThan(0.2)));


    }

    @Test
    public void shouldResolveMultipleAddressesAndProvideUpdatesWhenOneDisconnects() throws Exception {

        AtomicInteger server1Count = new AtomicInteger();
        AtomicInteger server2Count = new AtomicInteger();

        Server server1 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                server1Count.incrementAndGet();
                responseObserver.onNext(Health.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server1.start();

        int server1Port = server1.getPort();
        Server server2 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                server2Count.incrementAndGet();
                responseObserver.onNext(Health.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server2.start();
        int server2Port = server2.getPort();

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps = new ZookeeperServiceRegistrationOps(zkHost);
        zookeeperServiceRegistrationOps.removeServiceRegistry("pingPongService");
        //Register server1 providing pingPongServer
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server1Port));
        //Register server2 providing pingPongServer
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server2Port));

        //build a channel to targeted service
        ManagedChannel channel = ManagedChannelBuilder.forTarget("zk://pingPongService")
                .nameResolverFactory(ZookeeperZoneAwareNameResolverProvider.newBuilder()
                        .setZookeeperAddress(zkHost)
                        .build())
                .usePlaintext(true)
                .loadBalancerFactory(HealthAwareLoadBalancerFactory.withRoundRobinOnly()).build();

        //pingit!
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
        for (int i = 0; i < 100; i++) {
            Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
            Assert.assertThat(pong, is(notNullValue()));
        }

        double weightedDifference = Math.abs(server1Count.get() - server2Count.get()) / (double) (server1Count.get() + server2Count.get());
        //check that the loadbalancer is not too skewed (less than 10% difference between the two server counts)
        Assert.assertThat(weightedDifference, is(lessThan(0.1)));

        //Simulate server2 disconnecting and de-registering
        zookeeperServiceRegistrationOps.deregister("pingPongService", URI.create("dns://localhost:" + server2Port));
        server1Count.set(0);
        server2Count.set(0);

        //pingit!
        for (int i = 0; i < 100; i++) {
            Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
            Assert.assertThat(pong, is(notNullValue()));
        }

        //we de-registered server2, so server1count should be >> server2count
        double weightedDifference2 = Math.abs(server1Count.get() - server2Count.get()) / (double) (server1Count.get() + server2Count.get());
        //check that the loadbalancer is very skewed (more than 90% of calls went to server1, cant be 100% because it takes some time for the watch to propagate to zk and back)
        Assert.assertThat(weightedDifference2, is(greaterThan(0.9)));
    }
}