package com.arnold.msg.data;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class OffsetTracker {

    private final SortedMap<Long, Long> completedRanges;
    private final AtomicLong watermark;

    public OffsetTracker(long currentOffset) {
        this.watermark = new AtomicLong(currentOffset);
        this.completedRanges = new ConcurrentSkipListMap<>();
    }

    public void addCompletedOffset(long startOffset, long endOffset) {
        completedRanges.put(startOffset, endOffset);
    }

    public synchronized boolean advanceWatermark() {
        boolean changed = false;
        while (true) {
            if (completedRanges.isEmpty()) {
                break;
            }
            Long smallestKey = completedRanges.firstKey();
            if (smallestKey > currentWatermark()) {
                break;
            }
            Long endOffset = completedRanges.get(smallestKey);
            watermark.set(endOffset + 1);
            completedRanges.remove(smallestKey);
            changed = true;
        }
        return changed;
    }

    public long currentWatermark() {
        return watermark.get();
    }
}
