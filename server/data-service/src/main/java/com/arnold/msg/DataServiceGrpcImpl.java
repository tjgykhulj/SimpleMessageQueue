package com.arnold.msg;

import com.arnold.msg.data.MessageClientPoolRegistry;
import com.arnold.msg.data.MessageConsumer;
import com.arnold.msg.data.MessageProducer;
import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.metadata.model.ClusterKind;
import com.arnold.msg.proto.data.*;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class DataServiceGrpcImpl extends DataServiceGrpc.DataServiceImplBase {

    @Override
    public void produce(ProduceRequest request,
                        StreamObserver<ProduceResponse> responseObserver) {
        try {
            String id = request.getProducer();
            ClusterKind kind = getClusterKind(id);
            MessageProducer producer = MessageClientPoolRegistry.getPool(kind).getProducer(id);
            List<Message> list = request.getMessagesList()
                    .stream()
                    .map(this::convertFromProto)
                    .toList();
            producer.send(list);
            // TODO may put the new stored messages list into the response
            ProduceResponse resp = ProduceResponse.newBuilder().setSuccess(true).build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to produce messages", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void consume(ConsumeRequest request,
                        StreamObserver<ConsumeResponse> responseObserver) {
        try {
            String id = request.getConsumer();
            ClusterKind kind = getClusterKind(id);
            MessageConsumer client = MessageClientPoolRegistry.getPool(kind).getConsumer(id);
            MessageBatch batch = client.poll();
            responseObserver.onNext(convertToProto(batch));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to produce messages", e);
            responseObserver.onError(e);
        }
    }

    private ConsumeResponse convertToProto(MessageBatch batch) {
        ConsumeResponse.Builder builder = ConsumeResponse.newBuilder().setBatchId(batch.getBatchId());
        for (Message message : batch.getMessages()) {
            builder.addMessages(convertToProto(message));
        }
        return builder.build();
    }

    private MessageProto convertToProto(Message message) {
        return  MessageProto.newBuilder()
                .setMessageId(message.getMessageId())
                .setQueue(message.getQueue())
                .setPayload(ByteString.copyFrom(message.getPayload()))
                .build();
    }

    private ClusterKind getClusterKind(String producer) {
        // TODO only for test, need to find cluster kind by producer -> cluster -> clusterKind
        return ClusterKind.KAFKA;
    }

    private Message convertFromProto(MessageProto origin) {
        Message msg = new Message();
        msg.setQueue(origin.getQueue());
        msg.setPayload(origin.toByteArray());
        return msg;
    }
}