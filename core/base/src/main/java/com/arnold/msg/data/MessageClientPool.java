package com.arnold.msg.data;

import com.arnold.msg.metadata.model.ConsumerMetadata;
import com.arnold.msg.metadata.model.ProducerMetadata;

public interface MessageClientPool {

    MessageConsumer getConsumer(ConsumerMetadata consumer);
    MessageProducer getProducer(ProducerMetadata producer);

}
