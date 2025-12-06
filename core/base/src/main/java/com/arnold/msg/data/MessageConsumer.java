package com.arnold.msg.data;

import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.data.model.MessageBatchAck;

public interface MessageConsumer {
    MessageBatch poll();
    void acknowledge(MessageBatchAck batchAck);
}
