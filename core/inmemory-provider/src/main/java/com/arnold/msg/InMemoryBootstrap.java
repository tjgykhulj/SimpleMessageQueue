package com.arnold.msg;

import com.arnold.msg.data.InMemoryMessageClientPool;
import com.arnold.msg.data.MessageClientPoolRegistry;
import com.arnold.msg.metadata.model.ClusterKind;
import com.arnold.msg.metadata.model.ResourceType;
import com.arnold.msg.metadata.opeartor.BackendOperatorRegistry;
import com.arnold.msg.metadata.operator.InMemoryBackendOperator;
import com.arnold.msg.metadata.store.InMemoryMetadataStore;
import com.arnold.msg.metadata.store.MetadataStoreRegistry;

public class InMemoryBootstrap {

    public static void initAll() {
        initBackendOperator();
        initMetadataStore();
        initDataClient();
    }

    private static void initDataClient() {
        MessageClientPoolRegistry.registerPool(new InMemoryMessageClientPool());
    }

    public static void initMetadataStore() {
        for (ResourceType type : ResourceType.values()) {
            MetadataStoreRegistry.registerMetadataStore(type, new InMemoryMetadataStore());
        }
    }

    public static void initBackendOperator() {
        BackendOperatorRegistry.registerOperator(ClusterKind.IN_MEMORY,
                new InMemoryBackendOperator());
    }
}
