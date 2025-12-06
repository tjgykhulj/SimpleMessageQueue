package com.arnold.msg.data;

import com.arnold.msg.data.model.Message;
import com.arnold.msg.exceptions.MessageSendException;
import com.arnold.msg.metadata.model.KafkaClusterConfig;
import com.arnold.msg.metadata.model.ProducerMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class KafkaMessageProducer implements MessageProducer {

    private final Producer<byte[], byte[]> client;
    private final String id;
    private final int timeoutMs;

    public KafkaMessageProducer(KafkaClusterConfig config, ProducerMetadata producer) {
        // TODO make this configurable in ProducerMetadata
        this.timeoutMs = 5000;
        Properties props = config.toProperties();
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timeoutMs);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, producer.getId());
        this.client = new KafkaProducer<>(props);
        this.id = producer.getId();
        log.info("producer created: {}", producer);
    }

    @Override
    public void send(Message message) {
        send(Collections.singletonList(message));
    }

    @Override
    public void send(List<Message> messageList) {
        log.debug("Producer {} send messages with size {}", id, messageList.size());
        long waitUntil = System.currentTimeMillis() + timeoutMs;
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (Message msg: messageList) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(msg.getQueue(), msg.getPayload());
            futures.add(client.send(record));
        }
        for (Future<RecordMetadata> future: futures) {
            try {
                long waitTime = waitUntil - System.currentTimeMillis();
                if (waitTime <= 0) {
                    throw new MessageSendException("Timeout when sending messages");
                }
                future.get(waitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessageSendException("Thread interrupted when sending messages", e);
            } catch (ExecutionException e) {
                // TODO process this type of exception more precisely
                throw new MessageSendException("ExecutionException when sending messages", e);
            } catch (TimeoutException e) {
                log.warn("Timeout when sending messages {}", this.timeoutMs, e);
                throw new MessageSendException("Timeout when sending messages", e);
            }
        }

    }
}
