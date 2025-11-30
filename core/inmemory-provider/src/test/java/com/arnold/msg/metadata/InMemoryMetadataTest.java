package com.arnold.msg.metadata;

import com.arnold.msg.InMemoryBootstrap;
import com.arnold.msg.metadata.model.ClusterKind;
import com.arnold.msg.metadata.model.ClusterMetadata;
import com.arnold.msg.metadata.model.QueueMetadata;
import com.arnold.msg.metadata.model.ResourceType;
import com.arnold.msg.metadata.opeartor.BackendOperator;
import com.arnold.msg.metadata.opeartor.BackendOperatorRegistry;
import com.arnold.msg.metadata.store.MetadataStore;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InMemoryMetadataTest {

    @Test
    public void testMetadata() {
        InMemoryBootstrap.initMetadataStore();
        MetadataStore<ClusterMetadata> clusterStore = MetadataStoreRegistry.getMetadataStore(ResourceType.CLUSTER);
        ClusterMetadata cluster = new ClusterMetadata();
        cluster.setId("test");
        cluster.setKind(ClusterKind.IN_MEMORY);
        cluster.setProvider("{}");
        clusterStore.save(cluster);

        ClusterMetadata findCluster = clusterStore.findByID(cluster.getId());
        Assertions.assertNotNull(findCluster);
        Assertions.assertEquals(cluster.toString(), findCluster.toString());

        ClusterMetadata deletedCluster = clusterStore.deleteByID(cluster.getId());
        Assertions.assertNotNull(deletedCluster);
        Assertions.assertEquals(cluster.toString(), deletedCluster.toString());
    }

    @Test
    public void testOperator() {
        InMemoryBootstrap.initBackendOperator();
        BackendOperator operator = BackendOperatorRegistry.getOperator(ClusterKind.IN_MEMORY);

        QueueMetadata queue = new QueueMetadata();
        queue.setId("test");
        ClusterMetadata cluster = new ClusterMetadata();
        operator.createQueue(cluster, queue);
        operator.deleteQueue(cluster, queue.getId());
    }
}
