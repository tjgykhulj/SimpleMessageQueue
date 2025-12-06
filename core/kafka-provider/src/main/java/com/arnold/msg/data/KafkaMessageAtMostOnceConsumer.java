package com.arnold.msg.data;

import com.arnold.msg.Utils;
import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.data.model.MessageBatchAck;
import com.arnold.msg.metadata.model.ConsumerMetadata;
import com.arnold.msg.metadata.model.KafkaClusterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class KafkaMessageAtMostOnceConsumer implements MessageConsumer {

    private final Consumer<byte[], byte[]> client;
    private final String id;
    private final String queue;
    private final int timeoutMs;

    public KafkaMessageAtMostOnceConsumer(KafkaClusterConfig clusterConfig, ConsumerMetadata consumer) {
        // TODO make this configurable in ConsumerMetadata
        this.timeoutMs = 5000;
        Properties props = clusterConfig.toProperties();
        // TODO configured in metadata
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.getId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.client = new KafkaConsumer<>(props);
        this.queue = consumer.getQueue();
        this.client.subscribe(Collections.singleton(this.queue));
        this.id = consumer.getId();
        log.info("consumer client created: {}", consumer);
    }

    @Override
    public MessageBatch poll() {
        MessageBatch batch = new MessageBatch();
        batch.setMessages(new ArrayList<>());
        batch.setBatchId(UUID.randomUUID().toString());
        ConsumerRecords<byte[], byte[]> records = client.poll(Duration.ofMillis(timeoutMs));
        for (ConsumerRecord<byte[], byte[]> record : records.records(queue)) {
            Message msg = new Message();
            msg.setQueue(queue);
            msg.setPayload(record.value());
            msg.setMessageId(Utils.generateMessageID(record.partition(), record.offset()));
            batch.getMessages().add(msg);
        }
        return batch;
    }

    @Override
    public void acknowledge(MessageBatchAck batchAck) {
        // do nothing in at most once semantic
        log.debug("ack consumer {} with {}", this.id, batchAck);
    }
}
