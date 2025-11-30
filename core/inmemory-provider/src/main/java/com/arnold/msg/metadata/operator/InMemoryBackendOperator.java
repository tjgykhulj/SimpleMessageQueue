package com.arnold.msg.metadata.operator;

import com.arnold.msg.data.InMemoryMessageData;
import com.arnold.msg.metadata.model.ClusterMetadata;
import com.arnold.msg.metadata.model.QueueMetadata;
import com.arnold.msg.metadata.opeartor.BackendOperator;

import java.util.LinkedList;

public class InMemoryBackendOperator implements BackendOperator {

    @Override
    public void createQueue(ClusterMetadata cluster, QueueMetadata queue) {
        InMemoryMessageData.DATA.putIfAbsent(queue.getId(), new LinkedList<>());
    }

    @Override
    public void deleteQueue(ClusterMetadata cluster, String name) {
        InMemoryMessageData.DATA.remove(name);
    }
}
