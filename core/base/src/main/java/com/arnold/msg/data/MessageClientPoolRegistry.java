package com.arnold.msg.data;

import com.arnold.msg.exceptions.NotInitializeException;

public class MessageClientPoolRegistry {

    private static MessageClientPool pool;

    public static void registerPool(MessageClientPool input) {
        pool = input;
    }

    public static MessageClientPool getPool() {
        if (pool == null) {
            throw new NotInitializeException("MessageClientPool has not been initialized");
        }
        return pool;
    }
}
