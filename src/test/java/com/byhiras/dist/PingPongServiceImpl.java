package com.byhiras.dist;

import io.grpc.stub.StreamObserver;

import com.byhiras.dist.common.Common;
import com.byhiras.dist.common.PingPongGrpc;

/**
 * Author stefanofranz
 */
public class PingPongServiceImpl extends PingPongGrpc.PingPongImplBase {


    @Override
    public void pingit(Common.Ping request, StreamObserver<Common.Pong> responseObserver) {
        responseObserver.onNext(Common.Pong.newBuilder().build());
        responseObserver.onCompleted();
    }
}
