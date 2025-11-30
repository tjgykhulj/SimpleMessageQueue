package com.arnold.msg.server;

import com.arnold.msg.proto.common.ClusterInfo;
import com.arnold.msg.proto.common.ClusterKindEnum;
import com.arnold.msg.proto.common.QueueInfo;
import com.arnold.msg.proto.meta.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class ClientIntegrationTest {

    private final MetaServiceGrpc.MetaServiceBlockingStub blockingStub;

    private static final String cluster = "test-cluster";
    private static final String queue = "test-queue";

    public ClientIntegrationTest() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 9000)
                .usePlaintext()
                .build();
        this.blockingStub = MetaServiceGrpc.newBlockingStub(channel);
    }

    @Test
    public void testCreateCluster() {
        CreateClusterRequest request = CreateClusterRequest.newBuilder()
                .setCluster(ClusterInfo.newBuilder()
                        .setName(cluster)
                        .setKind(ClusterKindEnum.IN_MEMORY)
                        .build())
                .build();
        ClusterResponse response = blockingStub.createCluster(request);
        System.out.println("CreateCluster Response: " + response);
    }

    @Test
    public void testDeleteCluster() {
        DeleteClusterRequest deleteReq = DeleteClusterRequest.newBuilder()
                .setName(cluster)
                .build();
        ClusterResponse deleteResp = blockingStub.deleteCluster(deleteReq);
        System.out.println("DeleteCluster Response: " + deleteResp);
    }

    @Test
    public void testCreateQueue() {
        CreateQueueRequest req = CreateQueueRequest.newBuilder()
                .setQueue(QueueInfo.newBuilder()
                        .setName(queue)
                        .setCluster(cluster)
                        .build())
                .build();
        QueueResponse response = blockingStub.createQueue(req);
        log.info("CreateQueue Response: {}", response);
    }

    @Test
    public void testDeleteQueue() {
        DeleteQueueRequest req = DeleteQueueRequest.newBuilder().setName(queue).build();
        QueueResponse resp = blockingStub.deleteQueue(req);
        log.info("DeleteQueue Response: {}", resp);
    }
}
