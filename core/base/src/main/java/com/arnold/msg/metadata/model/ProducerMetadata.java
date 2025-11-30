package com.arnold.msg.metadata.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProducerMetadata implements Metadata {
    private String id;

    @Override
    public ResourceType getResourceType() {
        return ResourceType.PRODUCER;
    }
}
