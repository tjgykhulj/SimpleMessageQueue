package com.arnold.msg.metadata.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsumerMetadata implements Metadata {

    private String id;
    private String queue;
    private String cluster;
    private DeliverySemantic semantic = DeliverySemantic.AT_LEAST_ONCE;

    @Override
    public ResourceType getResourceType() {
        return ResourceType.CONSUMER;
    }
}
