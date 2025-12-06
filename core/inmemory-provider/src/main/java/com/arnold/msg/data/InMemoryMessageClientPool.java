package com.arnold.msg.data;

import com.arnold.msg.metadata.model.ConsumerMetadata;
import com.arnold.msg.metadata.model.ResourceType;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;

public class InMemoryMessageClientPool implements MessageClientPool {

    @Override
    public MessageConsumer getConsumer(String consumer) {
        ConsumerMetadata metadata = MetadataStoreRegistry.<ConsumerMetadata>getMetadataStore(ResourceType.CONSUMER)
                .findByID(consumer);
        return new InMemoryAtMostOnceMessageConsumer(consumer, metadata.getQueue());
    }

    @Override
    public MessageProducer getProducer(String producer) {
        return new InMemoryMessageProducer();
    }
}
