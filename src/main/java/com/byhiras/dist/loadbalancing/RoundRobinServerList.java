/*
 * Copyright 2015, Google Inc. All rights reserved.
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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import io.grpc.EquivalentAddressGroup;
import io.grpc.TransportManager;

/**
 * Manages a list of server addresses to round-robin on.
 */
@ThreadSafe
public class RoundRobinServerList<T> extends ServerList<T>{

    private final AtomicLong currentIdx = new AtomicLong();

    private RoundRobinServerList(TransportManager<T> tm, List<EquivalentAddressGroup> list) {
        super(tm, list);
    }

    /**
     * Returns the next transport in the list of servers.
     *
     * @return the next transport
     */
    public T getTransportForNextServer() {
        if (list.isEmpty()){
            return requestDroppingTransport;
        }else{
            return tm.getTransport(list.get((int)(currentIdx.getAndIncrement() % list.size())));
        }
    }

    @NotThreadSafe
    public static class Builder<T> extends ServerList.Builder<T>{
        public Builder(TransportManager<T> tm) {
            super(tm);
        }
        public RoundRobinServerList<T> build() {
            return new RoundRobinServerList<T>(tm, listBuilder.build());
        }
    }
}
