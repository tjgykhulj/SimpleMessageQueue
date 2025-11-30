package com.arnold.msg.data;

import com.arnold.msg.InMemoryBootstrap;
import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.data.model.MessageBatchAck;
import com.arnold.msg.metadata.model.*;
import com.arnold.msg.metadata.opeartor.BackendOperator;
import com.arnold.msg.metadata.opeartor.BackendOperatorRegistry;
import com.arnold.msg.metadata.operator.InMemoryBackendOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryMessageDataTest {

    @Test
    public void test() {
        InMemoryBootstrap.initAll();
        BackendOperator operator = BackendOperatorRegistry.getOperator(ClusterKind.IN_MEMORY);
        MessageClientPool pool = MessageClientPoolRegistry.getPool();

        String queue = "test";
        QueueMetadata queueMetadata = new QueueMetadata();
        queueMetadata.setId(queue);
        operator.createQueue(new ClusterMetadata(), queueMetadata);

        Message msg = new Message();
        msg.setData("test".getBytes());
        msg.setQueue(queue);
        MessageProducer producer = pool.getProducer(new ProducerMetadata());
        producer.send(msg);

        ConsumerMetadata metadata1 = new ConsumerMetadata();
        metadata1.setId("testConsumer1");
        metadata1.setQueue(queue);
        MessageConsumer consumer = pool.getConsumer(metadata1);
        MessageBatch batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertEquals(1, batch.getMessages().size());
        assertEquals(msg, batch.getMessages().get(0));

        // poll again by the same consumer, nothing it can poll
        batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertTrue(batch.getMessages().isEmpty());

        // poll again by a new consumer id, it should poll 1 message from the beginning as expect
        ConsumerMetadata metadata2 = new ConsumerMetadata();
        metadata2.setId("testConsumer2");
        metadata2.setQueue(queue);
        consumer = pool.getConsumer(metadata2);
        batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertEquals(1, batch.getMessages().size());
    }
}
