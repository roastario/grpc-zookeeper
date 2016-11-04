/*
 * Copyright 2016, Google Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.byhiras.dist.loadbalancing;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.concurrent.GuardedBy;

import io.grpc.Attributes;
import io.grpc.Channel;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ExperimentalApi;
import io.grpc.LoadBalancer;
import io.grpc.ResolvedServerInfo;
import io.grpc.Status;
import io.grpc.TransportManager;
import io.grpc.TransportManager.InterimTransport;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.netty.NettyChannelBuilder;


/**
 * Shameless copy and paste of the {@link io.grpc.util.RoundRobinLoadBalancerFactory.RoundRobinLoadBalancer}
 * with extensions for arbitary "health" checks for a given set of addresses.
 * uses a scheduled executor to periodically check the health of the resolved address
 * and filters out those which fail the check.
 */
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/1771")
@SuppressWarnings("unchecked")
public final class HealthAwareLoadBalancerFactory extends LoadBalancer.Factory {

    private final Predicate<Channel> healthFunction;
    private final long checkingFrequencySize;
    private final TimeUnit checkingFrequencyUnit;
    private final Function<TransportManager, ServerList.Builder> listBuilderSupplier;


    public static HealthAwareLoadBalancerFactory withHealthCheckOnly(Predicate<Channel> healthFunction,
                                                                     long checkingFrequencySize,
                                                                     TimeUnit checkingFrequencyUnit) {
        return new HealthAwareLoadBalancerFactory(healthFunction, checkingFrequencySize, checkingFrequencyUnit, ServerList.Builder::new);
    }

    public static HealthAwareLoadBalancerFactory withHealthCheckAndRoundRobin(Predicate<Channel> healthFunction,
                                                                              long checkingFrequencySize,
                                                                              TimeUnit checkingFrequencyUnit) {
        return new HealthAwareLoadBalancerFactory(healthFunction, checkingFrequencySize, checkingFrequencyUnit, RoundRobinServerList.Builder::new);
    }

    public static HealthAwareLoadBalancerFactory withRoundRobinOnly() {
        return new HealthAwareLoadBalancerFactory(equivalentAddressGroups -> true, -1, TimeUnit.MILLISECONDS, RoundRobinServerList.Builder::new);
    }

    private HealthAwareLoadBalancerFactory(Predicate<Channel> healthFunction,
                                           long checkingFrequencySize,
                                           TimeUnit checkingFrequencyUnit,
                                           Function<TransportManager, ServerList.Builder> listBuilderSupplier) {
        this.healthFunction = healthFunction;
        this.checkingFrequencySize = checkingFrequencySize;
        this.checkingFrequencyUnit = checkingFrequencyUnit;
        this.listBuilderSupplier = listBuilderSupplier;
    }

    @Override
    public <T> LoadBalancer<T> newLoadBalancer(String serviceName,
                                               TransportManager<T> tm) {
        return new HealthAwareLoadBalancer<T>(tm, healthFunction, checkingFrequencySize, checkingFrequencyUnit, listBuilderSupplier);
    }

    private static class HealthAwareLoadBalancer<T> extends LoadBalancer<T> {
        private static final Status SHUTDOWN_STATUS =
                Status.UNAVAILABLE.augmentDescription("HealthAwareRoundRobinLoadBalancer has shut down");

        private final Object lock = new Object();
        private final ScheduledExecutorService SHARED_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

        @GuardedBy("lock")
        private final AtomicReference<ServerList<T>> addressesToCheck = new AtomicReference<>(null);
        @GuardedBy("lock")
        private final AtomicReference<ServerList<T>> addressesToUse = new AtomicReference<>(null);
        @GuardedBy("lock")
        private InterimTransport<T> interimTransport;
        @GuardedBy("lock")
        private Status nameResolutionError;
        @GuardedBy("lock")
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final ConcurrentHashMap<SocketAddress, Channel> channelBuffer = new ConcurrentHashMap<>();


        private final TransportManager<T> tm;
        private final Predicate<Channel> healthChecker;
        private final Function<TransportManager<T>, ServerList.Builder<T>> listBuilderSupplier;

        private HealthAwareLoadBalancer(TransportManager<T> tm, Predicate<Channel> healthChecker,
                                        long checkingFrequencySize, TimeUnit checkingFrequencyUnit,
                                        Function listBuilderSupplier) {
            this.tm = tm;
            this.healthChecker = healthChecker;
            this.listBuilderSupplier = (Function<TransportManager<T>, ServerList.Builder<T>>) listBuilderSupplier;
            if (checkingFrequencySize > 0) {
                final Runnable checkingRunnable = () -> {
                    ServerList<T> toCheck = this.addressesToCheck.get();
                    ServerList.Builder<T> toUseBuilder = this.listBuilderSupplier.apply(tm);
                    if (toCheck != null && toCheck.size() > 0) {
                        for (EquivalentAddressGroup addressGroup : toCheck.getList()) {
                            getChannelForAddressGroup(addressGroup).ifPresent(channelToTest -> {
                                if (this.healthChecker.test(channelToTest)) {
                                    toUseBuilder.addList(addressGroup.getAddresses());
                                }
                            });
                        }
                    }
                    addressesToUse.set(toUseBuilder.build());
                };
                SHARED_EXECUTOR.scheduleWithFixedDelay(checkingRunnable, 0, checkingFrequencySize, checkingFrequencyUnit);
            }
        }


        private Optional<Channel> getChannelForAddressGroup(EquivalentAddressGroup addressGroup) {
            SocketAddress socketAddress = addressGroup.getAddresses().get(0);
            Channel channelToUse = channelBuffer.computeIfAbsent(socketAddress, (socketAddressToBuildFor) -> {
                Channel channel = null;
                for (SocketAddress address : addressGroup.getAddresses()) {
                    if (socketAddress instanceof InProcessSocketAddress) {
                        channel = InProcessChannelBuilder.forName(((InProcessSocketAddress) socketAddress).getName()).usePlaintext(true).build();
                    } else {
                        try {
                            channel = NettyChannelBuilder.forAddress(address).usePlaintext(true).build();
                        } catch (Exception e) {
                            // probably not a type we can handle
                        }
                    }
                    if (channel != null) {
                        return channel;
                    }
                }
                return null;
            });
            return channelToUse == null ? Optional.empty() : Optional.of(channelToUse);
        }

        @Override
        public T pickTransport(Attributes affinity) {
            if (closed.get()) {
                synchronized (lock) {
                    return tm.createFailingTransport(SHUTDOWN_STATUS);
                }
            }
            if (addressesToUse.get() == null || addressesToUse.get().size() == 0) {
                synchronized (lock) {
                    if (nameResolutionError != null) {
                        return tm.createFailingTransport(nameResolutionError);
                    } else if (addressesToCheck.get() != null && addressesToUse.get().size() == 0) {
                        return tm.createFailingTransport(Status.UNAVAILABLE);
                    }
                    if (interimTransport == null) {
                        interimTransport = tm.createInterimTransport();
                    }
                    return interimTransport.transport();
                }
            }
            return addressesToUse.get().getTransportForNextServer();
        }

        @Override
        public void handleResolvedAddresses(
                List<? extends List<ResolvedServerInfo>> updatedServers, Attributes config) {
            final InterimTransport<T> savedInterimTransport;
            final ServerList<T> addressesCopy;
            synchronized (lock) {
                //we have received a new set of servers, clear our channel buffers!
                channelBuffer.clear();
                if (closed.get()) {
                    return;
                }
                ServerList.Builder<T> listBuilder = this.listBuilderSupplier.apply(tm);
                for (List<ResolvedServerInfo> servers : updatedServers) {
                    if (servers.isEmpty()) {
                        continue;
                    }

                    final List<SocketAddress> socketAddresses = new ArrayList<>(servers.size());
                    for (ResolvedServerInfo server : servers) {
                        socketAddresses.add(server.getAddress());
                    }
                    listBuilder.addList(socketAddresses);
                }
                addressesToCheck.set(listBuilder.build());
                addressesToUse.set(listBuilder.build());
                addressesCopy = addressesToCheck.get();
                nameResolutionError = null;
                savedInterimTransport = interimTransport;
                interimTransport = null;
            }
            if (savedInterimTransport != null) {
                savedInterimTransport.closeWithRealTransports(addressesCopy::getTransportForNextServer);
            }
        }

        @Override
        public void handleNameResolutionError(Status error) {
            InterimTransport<T> savedInterimTransport;
            synchronized (lock) {
                if (closed.get()) {
                    return;
                }
                error = error.augmentDescription("Name resolution failed");
                savedInterimTransport = interimTransport;
                interimTransport = null;
                nameResolutionError = error;
            }
            if (savedInterimTransport != null) {
                savedInterimTransport.closeWithError(error);
            }
        }

        @Override
        public void shutdown() {
            InterimTransport<T> savedInterimTransport;
            synchronized (lock) {
                if (closed.get()) {
                    return;
                }
                closed.set(true);
                savedInterimTransport = interimTransport;
                interimTransport = null;
            }
            if (savedInterimTransport != null) {
                savedInterimTransport.closeWithError(SHUTDOWN_STATUS);
            }

            SHARED_EXECUTOR.shutdown();
        }


    }


}
