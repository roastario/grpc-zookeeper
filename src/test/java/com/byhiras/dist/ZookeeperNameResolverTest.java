package com.byhiras.dist;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.grpc.Attributes;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.RoundRobinLoadBalancerFactory;

import com.byhiras.dist.common.Common;
import com.byhiras.dist.common.PingPongGrpc;

/**
 * Author stefanofranz
 */
public class ZookeeperNameResolverTest {

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

        ManagedChannel channel = ManagedChannelBuilder.forTarget("zk://pingPongService").nameResolverFactory(new NameResolver.Factory() {
            @Nullable
            @Override
            public NameResolver newNameResolver(URI targetUri, Attributes params) {
                return new ZookeeperNameResolver(targetUri, zookeeperServiceRegistrationOps);
            }

            @Override
            public String getDefaultScheme() {
                return "zk";
            }
        }).usePlaintext(true).build();
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
        Common.Pong pong = stub.pingit(Common.Ping.newBuilder().build());
        Assert.assertThat(pong, is(notNullValue()));

    }

    @Test
    public void shouldResolveMultipleAddressesAndAllowLoadBalancing() throws Exception {

        AtomicInteger server1Count = new AtomicInteger();
        AtomicInteger server2Count = new AtomicInteger();

        Server server1 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
                server1Count.incrementAndGet();
                responseObserver.onNext(Common.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server1.start();

        int server1Port = server1.getPort();

        Server server2 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
                server2Count.incrementAndGet();
                responseObserver.onNext(Common.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server2.start();

        int server2Port = server2.getPort();

        ZookeeperServiceRegistrationOps zookeeperServiceRegistrationOps = new ZookeeperServiceRegistrationOps(zkHost);
        zookeeperServiceRegistrationOps.removeServiceRegistry("pingPongService");
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server1Port));
        zookeeperServiceRegistrationOps.registerService("pingPongService", URI.create("dns://localhost:" + server2Port));

        ManagedChannel channel = ManagedChannelBuilder.forTarget("zk://pingPongService").nameResolverFactory(new NameResolver.Factory() {
            @Nullable
            @Override
            public NameResolver newNameResolver(URI targetUri, Attributes params) {
                return new ZookeeperNameResolver(targetUri, zookeeperServiceRegistrationOps);
            }

            @Override
            public String getDefaultScheme() {
                return "zk";
            }
        }).usePlaintext(true).loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance()).build();
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);

        for (int i = 0; i < 1000; i++) {
            Common.Pong pong = stub.pingit(Common.Ping.newBuilder().build());
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
            public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
                server1Count.incrementAndGet();
                responseObserver.onNext(Common.Pong.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }).build();
        server1.start();

        int server1Port = server1.getPort();
        Server server2 = ServerBuilder.forPort(0).addService(new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
                server2Count.incrementAndGet();
                responseObserver.onNext(Common.Pong.getDefaultInstance());
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
                .nameResolverFactory(new ZookeeperNameResolver.ZookeeperNameResolverProvider(zkHost))
                .usePlaintext(true)
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance()).build();

        //pingit!
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
        for (int i = 0; i < 100; i++) {
            Common.Pong pong = stub.pingit(Common.Ping.newBuilder().build());
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
            Common.Pong pong = stub.pingit(Common.Ping.newBuilder().build());
            Assert.assertThat(pong, is(notNullValue()));
        }

        //we de-registered server2, so server1count should be >> server2count
        double weightedDifference2 = Math.abs(server1Count.get() - server2Count.get()) / (double) (server1Count.get() + server2Count.get());
        //check that the loadbalancer is very skewed (more than 90% of calls went to server1, cant be 100% because it takes some time for the watch to propagate to zk and back)
        Assert.assertThat(weightedDifference2, is(greaterThan(0.9)));
    }
}