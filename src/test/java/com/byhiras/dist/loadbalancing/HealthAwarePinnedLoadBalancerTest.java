package com.byhiras.dist.loadbalancing;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import io.grpc.LoadBalancer;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import com.byhiras.dist.common.Health;
import com.byhiras.dist.common.PingPongGrpc;
import com.byhiras.dist.test.CounterGrpc;
import com.byhiras.dist.test.TestObjects;

/**
 * Author stefanofranz
 */
public class HealthAwarePinnedLoadBalancerTest extends HealthAwareRoundRobinLoadBalancerTest{

    private final Supplier<LoadBalancer.Factory> FACTORY_SUPPLIER = () -> {
        return HealthAwareLoadBalancerFactory.withHealthCheckOnly((channel) -> {
            try {
                PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
                Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
                Assert.assertThat(pong, is(notNullValue()));
                return pong != null;
            } catch (Exception e) {
                return false;
            }

        }, 5, TimeUnit.MILLISECONDS);
    };

    @Test
    public void shouldFilterOutAddressesWhichFailThePredicate() throws Exception {

        TestServerSetup testServerSetup = new TestServerSetup().invoke(FACTORY_SUPPLIER);
        AtomicBoolean server2Fail = testServerSetup.getServer2Fail();
        AtomicInteger server1Count = testServerSetup.getServer1Count();
        AtomicInteger server2Count = testServerSetup.getServer2Count();
        ManagedChannel requestChannel = testServerSetup.getRequestChannel();
        CounterGrpc.CounterBlockingStub counterBlockingStub = CounterGrpc.newBlockingStub(requestChannel);

        while (server1Count.get() < 500 && server2Count.get() < 500) {
            TestObjects.Counted counted = counterBlockingStub.countIt(TestObjects.Count.getDefaultInstance());
            Assert.assertThat(counted , is(Matchers.notNullValue()));
        }

        double weightedDifference = Math.abs(server1Count.get() - server2Count.get()) / ((float) server1Count.get() + server2Count.get());
        //should be 1
        Assert.assertThat(weightedDifference, is(closeTo(1.0, 0.01)));
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

        requestChannel.shutdownNow();
    }

    @Test(expected = StatusRuntimeException.class)
    public void shouldThrowUnavailableExceptionWhenAllEndPointsAreFailing() throws Exception {
        TestServerSetup testServerSetup = new TestServerSetup().invoke(FACTORY_SUPPLIER);
        AtomicBoolean server1Fail = testServerSetup.getServer1Fail();
        AtomicBoolean server2Fail = testServerSetup.getServer2Fail();
        ManagedChannel requestChannel = testServerSetup.getRequestChannel();
        server1Fail.set(true);
        server2Fail.set(true);
        CounterGrpc.CounterBlockingStub counterBlockingStub = CounterGrpc.newBlockingStub(requestChannel);
        for (int i = 0; i < 500; i++) {
            TestObjects.Counted counted = counterBlockingStub.countIt(TestObjects.Count.getDefaultInstance());
            Assert.assertThat(counted , is(Matchers.notNullValue()));
        }
        requestChannel.shutdownNow();
    }


}