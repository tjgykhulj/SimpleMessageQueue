package com.arnold.msg.data;

import com.arnold.msg.data.model.Message;
import com.arnold.msg.data.model.MessageBatch;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class SimpleMessageBatch {
    private String batchId;
    private List<Long> messageIds;

    public SimpleMessageBatch(MessageBatch batch) {
        this.batchId = batch.getBatchId();
        this.messageIds = batch.getMessages().stream().map(Message::getMessageId).toList();
    }
}
