package com.arnold.msg.data;

import com.arnold.msg.InMemoryProviderBootstrap;
import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.metadata.model.*;
import com.arnold.msg.metadata.opeartor.BackendOperator;
import com.arnold.msg.metadata.opeartor.BackendOperatorRegistry;
import com.arnold.msg.metadata.store.MetadataStore;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class InMemoryMessageDataTest {

    @Test
    public void test() {
        InMemoryProviderBootstrap.initAll();
        BackendOperator operator = BackendOperatorRegistry.getOperator(ClusterKind.IN_MEMORY);
        MessageClientPool pool = MessageClientPoolRegistry.getPool(ClusterKind.IN_MEMORY);

        String queue = "test";
        QueueMetadata queueMetadata = new QueueMetadata();
        queueMetadata.setId(queue);
        operator.createQueue(new ClusterMetadata(), queueMetadata);

        MetadataStore<ConsumerMetadata> consumerStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CONSUMER);
        ConsumerMetadata consumer1 = new ConsumerMetadata();
        consumer1.setQueue(queue);
        consumer1.setId("testConsumer1");
        consumerStore.save(consumer1);
        ConsumerMetadata consumer2 = new ConsumerMetadata();
        consumer2.setQueue(queue);
        consumer2.setId("testConsumer2");
        consumerStore.save(consumer2);

        Message msg = new Message();
        msg.setPayload("test".getBytes());
        msg.setQueue(queue);
        MessageProducer producer = pool.getProducer("p");
        producer.send(msg);

        MessageConsumer consumer = pool.getConsumer(consumer1.getId());
        MessageBatch batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertEquals(1, batch.getMessages().size());
        assertEquals(msg, batch.getMessages().get(0));

        // poll again by the same consumer, nothing it can poll
        batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertTrue(batch.getMessages().isEmpty());

        // poll again by a new consumer id, it should poll 1 message from the beginning as expect
        consumer = pool.getConsumer(consumer2.getId());
        batch = consumer.poll();
        assertNotNull(batch.getMessages());
        assertEquals(1, batch.getMessages().size());
    }
}
