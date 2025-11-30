package com.arnold.msg.metadata.store;

import com.arnold.msg.metadata.model.Metadata;
import com.arnold.msg.metadata.model.ResourceType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryMetadataStore implements MetadataStore<Metadata> {

    private final Map<String, Metadata> metadataMap;

    public InMemoryMetadataStore() {
        this.metadataMap = new ConcurrentHashMap<>();
    }

    @Override
    public Metadata save(Metadata metadata) {
        metadataMap.put(metadata.getId(), metadata);
        return metadata;
    }

    @Override
    public Metadata deleteByID(String id) {
        return metadataMap.remove(id);
    }

    @Override
    public Metadata findByID(String id) {
        return metadataMap.get(id);
    }
}
