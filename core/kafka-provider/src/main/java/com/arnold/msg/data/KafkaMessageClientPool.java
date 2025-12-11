package com.arnold.msg.data;

import com.arnold.msg.metadata.model.*;
import com.arnold.msg.metadata.store.MetadataStore;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaMessageClientPool implements MessageClientPool {

    private final Map<String, MessageProducer> producerPool;
    private final Map<String, MessageConsumer> consumerPool;

    private final MetadataStore<ProducerMetadata> producerStore;
    private final MetadataStore<ConsumerMetadata> consumerStore;
    private final MetadataStore<ClusterMetadata> clusterStore;

    public KafkaMessageClientPool() {
        this.producerPool = new ConcurrentHashMap<>();
        this.consumerPool = new ConcurrentHashMap<>();
        this.producerStore = MetadataStoreRegistry.getMetadataStore(ResourceType.PRODUCER);
        this.consumerStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CONSUMER);
        this.clusterStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CLUSTER);
    }

    @Override
    public MessageConsumer getConsumer(String consumer) {
        return consumerPool.computeIfAbsent(consumer, c -> {
            ConsumerMetadata metadata = consumerStore.findByID(c);
            ClusterMetadata cluster = clusterStore.findByID(metadata.getCluster());
            KafkaClusterMetadata kafkaCluster = new KafkaClusterMetadata(cluster);
            if (metadata.getSemantic() == DeliverySemantic.AT_MOST_ONCE) {
                return new KafkaMessageAtMostOnceConsumer(kafkaCluster.getConfig(), metadata);
            } else {
                return new KafkaMessageAtLeastOnceConsumer(kafkaCluster.getConfig(), metadata);
            }
        });
    }

    @Override
    public MessageProducer getProducer(String producer) {
        return producerPool.computeIfAbsent(producer, p -> {
            ProducerMetadata metadata = producerStore.findByID(p);
            ClusterMetadata cluster = clusterStore.findByID(metadata.getCluster());
            KafkaClusterMetadata kafkaCluster = new KafkaClusterMetadata(cluster);
            return new KafkaMessageProducer(kafkaCluster.getConfig(), metadata);
        });
    }
}
