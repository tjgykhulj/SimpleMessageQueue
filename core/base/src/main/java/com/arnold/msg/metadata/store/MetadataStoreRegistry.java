package com.arnold.msg.metadata.store;

import com.arnold.msg.exceptions.NotInitializeException;
import com.arnold.msg.metadata.model.Metadata;
import com.arnold.msg.metadata.model.ResourceType;

import java.util.HashMap;
import java.util.Map;

public class MetadataStoreRegistry {

    private static final Map<ResourceType, MetadataStore<?>> METADATA_STORE_MAP = new HashMap<>();

    public static <T extends Metadata> void registerMetadataStore(ResourceType type, MetadataStore<T> metadataStore) {
        METADATA_STORE_MAP.put(type, metadataStore);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Metadata> MetadataStore<T> getMetadataStore(ResourceType type) {
        if (!METADATA_STORE_MAP.containsKey(type)) {
            throw new NotInitializeException("MetadataStore has not been initialize with: " + type);
        }
        return (MetadataStore<T>) METADATA_STORE_MAP.get(type);
    }
}
