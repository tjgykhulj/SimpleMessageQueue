package com.arnold.msg.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OffsetTrackerTest {

    @Test
    public void test() {
        int off1 = 100;
        int off2 = 105;
        int off3 = 110;
        OffsetTracker tracker = new OffsetTracker(off1);
        tracker.addCompletedOffset(off2 + 1, off3);
        // advance failed, offset1 has not completed yet
        Assertions.assertFalse(tracker.advanceWatermark());
        tracker.addCompletedOffset(off1, off2);
        // advance from offset1 -> offset2+1 -> offset3+1
        Assertions.assertTrue(tracker.advanceWatermark());

        Assertions.assertEquals(off3 + 1, tracker.currentWatermark());
    }
}
