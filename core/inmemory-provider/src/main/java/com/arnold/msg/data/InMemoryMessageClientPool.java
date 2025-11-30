package com.arnold.msg.data;

import com.arnold.msg.metadata.model.ConsumerMetadata;
import com.arnold.msg.metadata.model.ProducerMetadata;

public class InMemoryMessageClientPool implements MessageClientPool {

    @Override
    public MessageConsumer getConsumer(ConsumerMetadata consumer) {
        return new InMemoryAtMostOnceMessageConsumer(consumer.getId(), consumer.getQueue());
    }

    @Override
    public MessageProducer getProducer(ProducerMetadata producer) {
        return new InMemoryMessageProducer();
    }
}
