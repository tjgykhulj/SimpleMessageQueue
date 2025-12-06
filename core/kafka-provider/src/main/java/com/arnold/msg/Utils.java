package com.arnold.msg;

public class Utils {

    private static final int RESERVED_BITS = 8;
    private static final int PARTITION_BITS = 12;
    private static final int OFFSET_BITS = 44;

    private static final long MAX_PARTITION = (1L << PARTITION_BITS) - 1;
    private static final long MAX_OFFSET = (1L << OFFSET_BITS) - 1;

    public static long generateMessageID(int partition, long offset) {
        if (partition < 0 || partition > MAX_PARTITION) {
            throw new IllegalArgumentException("Partition " + partition + " is out of range");
        }
        if (offset < 0 || offset > MAX_OFFSET) {
            throw new IllegalArgumentException("Offset: " + offset + " is out of range");
        }
        return ((long) partition << OFFSET_BITS) |
                (offset & ((1L << OFFSET_BITS) - 1));
    }
}
