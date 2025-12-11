package com.arnold.msg.service.meta;

import com.arnold.msg.exceptions.MetadataAlreadyExistException;
import com.arnold.msg.exceptions.MetadataNotFoundException;
import com.arnold.msg.metadata.model.*;
import com.arnold.msg.metadata.opeartor.BackendOperator;
import com.arnold.msg.metadata.opeartor.BackendOperatorRegistry;
import com.arnold.msg.metadata.store.MetadataStore;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;
import com.arnold.msg.proto.common.*;

import java.util.HashMap;

public class MetadataStoreService {

    private static MetadataStoreService INSTANCE;

    private final MetadataStore<QueueMetadata> queueStore;
    private final MetadataStore<ClusterMetadata> clusterStore;
    private final MetadataStore<ProducerMetadata> producerStore;
    private final MetadataStore<ConsumerMetadata> consumerStore;

    public static synchronized MetadataStoreService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MetadataStoreService();
        }
        return INSTANCE;
    }

    private MetadataStoreService() {
        queueStore = MetadataStoreRegistry.getMetadataStore(ResourceType.QUEUE);
        clusterStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CLUSTER);
        producerStore = MetadataStoreRegistry.getMetadataStore(ResourceType.PRODUCER);
        consumerStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CONSUMER);        
    }

    public ClusterInfo createCluster(ClusterInfo clusterInfo) {
        ClusterMetadata existCluster = clusterStore.findByID(clusterInfo.getName());
        if (existCluster != null) {
            throw new MetadataAlreadyExistException(clusterInfo.getName() + " already exist");
        }
        ClusterMetadata metadata = new ClusterMetadata();
        metadata.setId(clusterInfo.getName());
        metadata.setKind(ClusterKind.valueOf(clusterInfo.getKind().name()));
        metadata.setProvider(clusterInfo.getProviderMap());
        getOperator(metadata).validateCluster(metadata);
        clusterStore.save(metadata);
        return clusterInfo;
    }

    public ClusterInfo deleteCluster(String name) {
        ClusterMetadata metadata = clusterStore.deleteByID(name);
        if (metadata == null) {
            throw new MetadataNotFoundException("cluster not found: " + name);
        }
        return ClusterInfo.newBuilder()
                .setName(metadata.getId())
                .setKind(ClusterKindEnum.valueOf(metadata.getKind().name()))
                .putAllProvider(metadata.getProvider())
                .build();
    }

    public QueueInfo createQueue(QueueInfo info) {
        ClusterMetadata cluster = clusterStore.findByID(info.getCluster());
        if (cluster == null) {
            throw new MetadataNotFoundException("Cluster not found: " +info.getCluster());
        }
        QueueMetadata queue = queueStore.findByID(info.getName());
        if (queue != null) {
            throw new MetadataAlreadyExistException("Queue already exist: " + info.getName());
        }
        queue = convertFromProto(info);
        getOperator(cluster).createQueue(cluster, queue);
        queue = queueStore.save(queue);
        return convertToProto(queue);
    }

    public QueueInfo deleteQueue(String name) {
        QueueMetadata queue = queueStore.findByID(name);
        if (queue == null) {
            throw new MetadataNotFoundException("queue not found: " + name);
        }
        ClusterMetadata cluster = clusterStore.findByID(queue.getCluster());
        if (cluster == null) {
            throw new MetadataNotFoundException("cluster not found: " + queue.getCluster());
        }
        getOperator(cluster).deleteQueue(cluster, queue);
        QueueMetadata metadata = queueStore.deleteByID(name);
        return convertToProto(metadata);
    }

    public ProducerInfo createProducer(ProducerInfo info) {
        ClusterMetadata cluster = clusterStore.findByID(info.getCluster());
        if (cluster == null) {
            throw new MetadataNotFoundException("Cluster not found: " +info.getCluster());
        }
        ProducerMetadata metadata = producerStore.findByID(info.getName());
        if (metadata != null) {
            throw new MetadataAlreadyExistException("Queue already exist: " + info.getName());
        }
        metadata = convertFromProto(info);
        metadata = producerStore.save(metadata);
        return convertToProto(metadata);
    }

    public ConsumerInfo createConsumer(ConsumerInfo info) {
        ClusterMetadata cluster = clusterStore.findByID(info.getCluster());
        if (cluster == null) {
            throw new MetadataNotFoundException("Cluster not found: " +info.getCluster());
        }
        ConsumerMetadata metadata = consumerStore.findByID(info.getName());
        if (metadata != null) {
            throw new MetadataAlreadyExistException("Queue already exist: " + info.getName());
        }
        metadata = convertFromProto(info);
        metadata = consumerStore.save(metadata);
        return convertToProto(metadata);
    }

    private QueueMetadata convertFromProto(QueueInfo info) {
        if (info == null) {
            return null;
        }
        QueueMetadata queue = new QueueMetadata();
        queue.setId(info.getName());
        queue.setCluster(info.getCluster());
        return queue;
    }

    private QueueInfo convertToProto(QueueMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        return QueueInfo.newBuilder()
                .setName(metadata.getId())
                .setCluster(metadata.getCluster())
                .build();
    }

    private ProducerMetadata convertFromProto(ProducerInfo info) {
        if (info == null) {
            return null;
        }
        ProducerMetadata metadata = new ProducerMetadata();
        metadata.setId(info.getName());
        metadata.setCluster(info.getCluster());
        return metadata;
    }

    private ProducerInfo convertToProto(ProducerMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        return ProducerInfo.newBuilder()
                .setName(metadata.getId())
                .setCluster(metadata.getCluster())
                .build();
    }

    private ConsumerMetadata convertFromProto(ConsumerInfo info) {
        if (info == null) {
            return null;
        }
        ConsumerMetadata metadata = new ConsumerMetadata();
        metadata.setId(info.getName());
        metadata.setQueue(info.getQueue());
        metadata.setCluster(info.getCluster());
        metadata.setSemantic(DeliverySemantic.valueOf(info.getSemantic().name()));
        return metadata;
    }

    private ConsumerInfo convertToProto(ConsumerMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        return ConsumerInfo.newBuilder()
                .setName(metadata.getId())
                .setQueue(metadata.getQueue())
                .setCluster(metadata.getCluster())
                .setSemantic(DeliverySemanticEnum.valueOf(metadata.getSemantic().name()))
                .build();
    }

    private BackendOperator getOperator(ClusterMetadata cluster) {
        return BackendOperatorRegistry.getOperator(cluster.getKind());
    }
}
