package com.arnold.msg.data;

import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import com.arnold.msg.data.model.MessageBatchAck;

import java.util.*;

import static com.arnold.msg.data.InMemoryMessageData.DATA;
import static com.arnold.msg.data.InMemoryMessageData.OFFSETS;

public class InMemoryAtMostOnceMessageConsumer implements MessageConsumer {

    private final static int BATCH_MESSAGE_COUNT = 10;
    private final String consumer;
    private final String queue;


    public InMemoryAtMostOnceMessageConsumer(String consumer, String queue) {
        this.consumer = consumer;
        this.queue = queue;
    }

    @Override
    public synchronized MessageBatch poll() {
        List<Message> queueList = DATA.get(queue);
        if (queueList == null) {
            return new MessageBatch();
        }
        List<Message> messages = new ArrayList<>();
        for (int i=0; i<BATCH_MESSAGE_COUNT; i++) {
            Integer off = OFFSETS.getOrDefault(consumer, 0);
            if (queueList.size() <= off) {
                break;
            }
            messages.add(queueList.get(off));
            OFFSETS.put(consumer, off + 1);
        }
        MessageBatch batch = new MessageBatch();
        batch.setMessages(messages);
        batch.setBatchId(UUID.randomUUID().toString());
        return batch;
    }

    @Override
    public void acknowledge(MessageBatchAck batchAck) {
        // do nothing
    }
}
