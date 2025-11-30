package com.arnold.msg.metadata.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsumerMetadata implements Metadata {

    private String id;
    private String queue;

    @Override
    public ResourceType getResourceType() {
        return ResourceType.CONSUMER;
    }
}
