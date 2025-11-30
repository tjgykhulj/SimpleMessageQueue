package com.arnold.msg;

import com.arnold.msg.metadata.model.ClusterMetadata;
import com.arnold.msg.metadata.model.QueueMetadata;
import com.arnold.msg.metadata.model.ResourceType;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;
import com.arnold.msg.metadata.store.ZookeeperMetadataStore;

public class ZookeeperBootstrap {

    public static void initAll() {
        initClient();
        initMetadataStore();
    }

    public static void initClient() {
        ZookeeperClientHolder.initialize();
    }

    private static void initMetadataStore() {
        MetadataStoreRegistry.registerMetadataStore(ResourceType.QUEUE,
                new ZookeeperMetadataStore<>(ResourceType.QUEUE, QueueMetadata.class));
        MetadataStoreRegistry.registerMetadataStore(ResourceType.CLUSTER,
                new ZookeeperMetadataStore<>(ResourceType.CLUSTER, ClusterMetadata.class));
    }
}
