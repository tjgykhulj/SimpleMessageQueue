package com.arnold.msg.data.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Message {
    private byte[] payload;
    private String queue;
    private long messageId;
}
