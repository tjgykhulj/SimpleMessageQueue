package com.arnold.msg.server;

import com.arnold.msg.JsonUtils;
import com.arnold.msg.metadata.model.KafkaClusterConfig;
import com.arnold.msg.proto.common.*;
import com.arnold.msg.proto.data.*;
import com.arnold.msg.proto.meta.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ClientDataIntegrationTest {

    private final MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;
    private final DataServiceGrpc.DataServiceBlockingStub dataBlockingStub;

    private final String cluster = "local-cluster";
    private final String queue = "test-queue";
    private final String producer = "test-p";
    private final String consumer = "test-c";

    public ClientDataIntegrationTest() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 9000)
                .usePlaintext()
                .build();
        this.metaBlockingStub = MetaServiceGrpc.newBlockingStub(channel);
        ManagedChannel dataChannel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 9001)
                .usePlaintext()
                .build();
        this.dataBlockingStub = DataServiceGrpc.newBlockingStub(dataChannel);
    }

    @Test
    public void testCreateLocalCluster() {
        KafkaClusterConfig config = new KafkaClusterConfig();
        config.setBootstrapServers("localhost:9092");
        Map<String, String> provider = JsonUtils.fromObjToMap(config);
        log.info("provider = {}", provider);
        CreateClusterRequest request = CreateClusterRequest.newBuilder()
                .setCluster(ClusterInfo.newBuilder()
                        .setName(cluster)
                        .setKind(ClusterKindEnum.KAFKA)
                        .putAllProvider(provider)
                        .build())
                .build();
        ClusterInfo response = metaBlockingStub.createCluster(request);
        System.out.println("CreateCluster Response: " + response);
    }

    @Test
    public void testDeleteLocalCluster() {
        DeleteClusterRequest deleteReq = DeleteClusterRequest.newBuilder()
                .setName(cluster)
                .build();
        ClusterInfo deleteResp = metaBlockingStub.deleteCluster(deleteReq);
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
        QueueInfo response = metaBlockingStub.createQueue(req);
        log.info("CreateQueue Response: {}", response);
    }

    @Test
    public void testDeleteQueue() {
        DeleteQueueRequest req = DeleteQueueRequest.newBuilder().setName(queue).build();
        QueueInfo info = metaBlockingStub.deleteQueue(req);
        log.info("DeleteQueue Response: {}", info);
    }

    @Test
    public void testCreateProducer() {
        CreateProducerRequest req = CreateProducerRequest.newBuilder()
                .setProducer(ProducerInfo.newBuilder()
                        .setName(producer)
                        .setCluster(cluster)
                        .build())
                .build();
        ProducerInfo response = metaBlockingStub.createProducer(req);
        log.info("Create Produce Response: {}", response);
    }

    @Test
    public void testCreateConsumer() {
        CreateConsumerRequest req = CreateConsumerRequest.newBuilder()
                .setConsumer(ConsumerInfo.newBuilder()
                        .setName(consumer)
                        .setQueue(queue)
                        .setCluster(cluster)
                        .build())
                .build();
        ConsumerInfo response = metaBlockingStub.createConsumer(req);
        log.info("Create Consume Response: {}", response);
    }

    // after create cluster \ create queue
    @Test
    public void testProduce() {
        List<MessageProto> messages = new ArrayList<>();
        for (int i=0; i<100; i++) {
            MessageProto msg = MessageProto.newBuilder()
                    .setQueue(queue)
                    .setPayload(ByteString.copyFrom("test".getBytes()))
                    .build();
            messages.add(msg);
        }
        ProduceRequest req = ProduceRequest.newBuilder()
                .setProducer(producer)
                .addAllMessages(messages)
                .build();
        ProduceResponse resp = dataBlockingStub.produce(req);
        log.info("Produce Response: {}", resp);
    }

    @Test
    public void testConsume() {
        ConsumeRequest req = ConsumeRequest.newBuilder()
                .setConsumer(consumer)
                .build();
        ConsumeResponse resp = dataBlockingStub.consume(req);
        log.info("Consume Response: {}", resp);
    }
}
