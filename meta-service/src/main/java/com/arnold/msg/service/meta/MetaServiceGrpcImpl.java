package com.arnold.msg.service.meta;

import com.arnold.msg.proto.common.CommonProto;
import com.arnold.msg.proto.common.DataServer;
import com.arnold.msg.proto.common.QueueInfo;
import com.arnold.msg.proto.common.Response;
import com.arnold.msg.proto.meta.*;
import com.arnold.msg.registry.DataNode;
import com.arnold.msg.registry.NodeRegistry;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class MetaServiceGrpcImpl extends MetaServiceGrpc.MetaServiceImplBase {


    private final NodeRegistry nodeRegistry;

    public MetaServiceGrpcImpl(NodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public void createQueue(CreateQueueRequest request,
                            StreamObserver<CreateQueueResponse> responseObserver) {
        try {
            String queueName = request.getQueueName();

            // 创建队列逻辑
            QueueInfo queueInfo = createQueueInternal(queueName,
                    request.getPartitionCount(), request.getReplicationFactor());

            // 构建响应（使用生成的Builder）
            CreateQueueResponse response = CreateQueueResponse.newBuilder()
                            .setBase(createSuccessResponse())
                            .setQueueInfo(convertToProto(queueInfo))
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            handleError(responseObserver, e, "Failed to create queue");
        }
    }

    private QueueInfo createQueueInternal(String queueName, int partitionCount, int replicationFactor) {
        // TODO
        return QueueInfo.newBuilder().build();
    }

    @Override
    public void assignDataServer(AssignDataServerRequest request,
                                 StreamObserver<AssignDataServerResponse> responseObserver) {
        try {
            // TODO
            AssignDataServerResponse response =
                    AssignDataServerResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            handleError(responseObserver, e, "Failed to allocate data node");
        }
    }

    private DataServer convertToProto(DataNode node) {
        return DataServer.newBuilder()
                .setHost(node.getIp())
                .setPort(node.getPort())
                .build();
    }

    private QueueInfo convertToProto(QueueInfo queueInfo) {
        return QueueInfo.newBuilder()
                .setQueueName(queueInfo.getQueueName())
                .setPartitionCount(queueInfo.getPartitionCount())
                .setReplicationFactor(queueInfo.getReplicationFactor())
                .setCreatedTime(queueInfo.getCreatedTime())
                .setStatus(queueInfo.getStatus())
                .putAllProperties(queueInfo.getProperties())
                .build();
    }

    private Response createSuccessResponse() {
        return Response.newBuilder()
                .setSuccess(true)
                .setMessage("OK")
                .build();
    }

    private Response createErrorResponse(String message) {
        return Response.newBuilder()
                .setSuccess(false)
                .setMessage(message)
                .setErrorCode("INTERNAL_ERROR")
                .build();
    }

    private <T> void handleError(StreamObserver<T> responseObserver, Exception e, String context) {
        log.error("{}: {}", context, e.getMessage(), e);
        // TODO
    }


}