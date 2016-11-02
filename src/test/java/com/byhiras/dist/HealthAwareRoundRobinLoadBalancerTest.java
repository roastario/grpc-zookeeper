package com.byhiras.dist;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import io.grpc.Attributes;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.ResolvedServerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.byhiras.dist.common.Common;
import com.byhiras.dist.common.PingPongGrpc;
import com.byhiras.dist.test.CounterGrpc;
import com.byhiras.dist.test.TestObjects;

/**
 * Author stefanofranz
 */
public class HealthAwareRoundRobinLoadBalancerTest {

    @Test
    public void shouldFilterOutAddressesWhichFailThePredicate() throws Exception {

        TestServerSetup testServerSetup = new TestServerSetup().invoke();
        AtomicBoolean server2Fail = testServerSetup.getServer2Fail();
        AtomicInteger server1Count = testServerSetup.getServer1Count();
        AtomicInteger server2Count = testServerSetup.getServer2Count();
        ManagedChannel requestChannel = testServerSetup.getRequestChannel();

        CounterGrpc.CounterBlockingStub counterBlockingStub = CounterGrpc.newBlockingStub(requestChannel);

        for (int i = 0; i < 500; i++) {
            counterBlockingStub.countIt(TestObjects.Count.getDefaultInstance());
        }

        float weightedDifference = Math.abs(server1Count.get() - server2Count.get()) / ((float) server1Count.get() + server2Count.get());
        Assert.assertThat(weightedDifference, is(lessThan(0.2f)));
        server2Fail.set(true);
        //should update within
        Thread.sleep(10);
        resetCounters(server1Count, server2Count);
        for (int i = 0; i < 500; i++) {
            counterBlockingStub.countIt(TestObjects.Count.getDefaultInstance());
        }
        float weightedDifferenceFailingServer2 = Math.abs(server1Count.get() - server2Count.get()) / ((float) server1Count.get() + server2Count.get());
        Assert.assertThat(weightedDifferenceFailingServer2, is(greaterThan(0.8f)));
        Assert.assertThat(server1Count.get(), is(greaterThan(server2Count.get())));

    }

    @Test(expected = StatusRuntimeException.class)
    public void shouldThrowUnavailableExceptionWhenAllEndPointsAreFailing() throws Exception {
        TestServerSetup testServerSetup = new TestServerSetup().invoke();
        AtomicBoolean server1Fail = testServerSetup.getServer1Fail();
        AtomicBoolean server2Fail = testServerSetup.getServer2Fail();
        ManagedChannel requestChannel = testServerSetup.getRequestChannel();
        server1Fail.set(true);
        server2Fail.set(true);
        CounterGrpc.CounterBlockingStub counterBlockingStub = CounterGrpc.newBlockingStub(requestChannel);
        for (int i = 0; i < 500; i++) {
            counterBlockingStub.countIt(TestObjects.Count.getDefaultInstance());
        }
    }

    private ManagedChannel getChannelToServers(final int server1Port, final int server2Port, HealthAwareRoundRobinLoadBalancerFactory unitFactory) {
        return ManagedChannelBuilder.forTarget("DumDiDum://ignored").nameResolverFactory(new NameResolver.Factory() {
            @Nullable
            @Override
            public NameResolver newNameResolver(URI targetUri, Attributes params) {
                return new NameResolver() {
                    @Override
                    public String getServiceAuthority() {
                        return targetUri.getAuthority();
                    }

                    @Override
                    public void start(Listener listener) {
                        CompletableFuture.runAsync(() -> {
                            listener.onUpdate(asList(
                                    singletonList(new ResolvedServerInfo(new InetSocketAddress("localhost", server2Port), Attributes.EMPTY)),
                                    singletonList(new ResolvedServerInfo(new InetSocketAddress("localhost", server1Port), Attributes.EMPTY))
                            ), Attributes.EMPTY);
                        });
                    }

                    @Override
                    public void shutdown() {

                    }
                };
            }

            @Override
            public String getDefaultScheme() {
                return "DumDiDum";
            }
        }).loadBalancerFactory(unitFactory).usePlaintext(true).build();
    }

    private HealthAwareRoundRobinLoadBalancerFactory getLoadBalancerFactory() {
        return HealthAwareRoundRobinLoadBalancerFactory.withHealthChecker((channel) -> {
            try {
                PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
                Common.Pong pong = stub.pingit(Common.Ping.newBuilder().build());
                Assert.assertThat(pong, is(notNullValue()));
                return pong != null;
            } catch (Exception e) {
                return false;
            }

        }, 1, TimeUnit.MILLISECONDS);
    }

    private void resetCounters(AtomicInteger... counts) {
        for (AtomicInteger count : counts) {
            count.set(0);
        }
    }

    private PingPongGrpc.PingPongImplBase getFailingPingPongService(final AtomicBoolean fail) {
        return new PingPongGrpc.PingPongImplBase() {
            @Override
            public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
                if (fail.get()) {
                    responseObserver.onError(new RuntimeException("oh noes"));
                } else {
                    responseObserver.onNext(Common.Pong.getDefaultInstance());
                    responseObserver.onCompleted();
                }
            }
        };
    }

    private CounterGrpc.CounterImplBase getCountingService(final AtomicInteger integer) {
        return new CounterGrpc.CounterImplBase() {
            @Override
            public void countIt(TestObjects.Count request, StreamObserver<TestObjects.Counted> responseObserver) {
                integer.incrementAndGet();
                responseObserver.onNext(TestObjects.Counted.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    private class TestServerSetup {
        private AtomicInteger server1Count;
        private AtomicBoolean server1Fail;
        private AtomicInteger server2Count;
        private AtomicBoolean server2Fail;
        private ManagedChannel requestChannel;

        public AtomicInteger getServer1Count() {
            return server1Count;
        }

        public AtomicBoolean getServer1Fail() {
            return server1Fail;
        }

        public AtomicInteger getServer2Count() {
            return server2Count;
        }

        public AtomicBoolean getServer2Fail() {
            return server2Fail;
        }

        public ManagedChannel getRequestChannel() {
            return requestChannel;
        }

        public TestServerSetup invoke() throws IOException {
            server1Count = new AtomicInteger(0);
            server1Fail = new AtomicBoolean(false);
            Server server1 = ServerBuilder.forPort(0)
                    .addService(getFailingPingPongService(server1Fail))
                    .addService(getCountingService(server1Count))
                    .build();
            server1.start();
            int server1Port = server1.getPort();

            server2Count = new AtomicInteger(0);
            server2Fail = new AtomicBoolean(false);
            Server server2 = ServerBuilder.forPort(0)
                    .addService(getFailingPingPongService(server2Fail))
                    .addService(getCountingService(server2Count))
                    .build();
            server2.start();
            int server2Port = server2.getPort();

            HealthAwareRoundRobinLoadBalancerFactory unitFactory = getLoadBalancerFactory();
            requestChannel = getChannelToServers(server1Port, server2Port, unitFactory);
            return this;
        }
    }
}