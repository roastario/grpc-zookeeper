package com.byhiras.dist.healthchecking;

import io.grpc.stub.StreamObserver;

import com.byhiras.dist.common.Health;
import com.byhiras.dist.common.PingPongGrpc;

/**
 * Author stefanofranz
 */
public class PingPongServiceImpl extends PingPongGrpc.PingPongImplBase {


    @Override
    public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
        responseObserver.onNext(Health.Pong.newBuilder().build());
        responseObserver.onCompleted();
    }
}
