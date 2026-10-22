/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common.records;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.MessageMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerCheckPointTest {

    private static final String TEST_KEY_1 = "testKey1";
    private static final String TEST_KEY_2 = "testKey2";
    private ConsumerCheckPoint checkPoint;

    @BeforeMethod
    public void setup() {
        checkPoint = new ConsumerCheckPoint();
    }

    @Test
    public void testAddKey() {
        // Add the key to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));

        // Verify the key was added
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_1));
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
    }

    @Test
    public void testMergeKeepsRangesCreatedAtTheSameTime() {
        // Two Shared consumers created a range of the same key within one clock tick
        String timestamp = "2026-10-21T21:59:47.090196";
        checkPoint.addKey(TEST_KEY_1, createRangeMap(timestamp, 0L, 5L));
        ConsumerCheckPoint otherCheckPoint = new ConsumerCheckPoint();
        otherCheckPoint.addKey(TEST_KEY_1, createRangeMap(timestamp, 6L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("2026-10-21T21:59:47.090197", 11L, 12L));

        checkPoint.merge(otherCheckPoint);

        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 3);
        assertEquals(checkPoint.computeHighWatermarks().get(TEST_KEY_1).longValue(), 12L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).lastKey(), "2026-10-21T21:59:47.090197");
    }

    @Test
    public void testMerge() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));

        // Create second checkpoint with a different range
        ConsumerCheckPoint otherCheckPoint = new ConsumerCheckPoint();
        otherCheckPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        // Merge the checkpoints
        checkPoint.merge(otherCheckPoint);

        // Verify the merge
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("10").getStart().seq(), 10L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("10").getEnd().seq(), 15L);
    }

    @Test
    public void testGetLastSeq() {
        // Add ranges to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 10L, 12L));

        // Verify getLastSeq returns the highest sequence
        LongSeq lastSeq = checkPoint.getLastSeq(TEST_KEY_1);
        assertNotNull(lastSeq);
        assertEquals(lastSeq.seq(), 15L);

        // Test with non-existent key
        assertNull(checkPoint.getLastSeq("nonExistentKey"));
    }

    @Test
    public void testCalculateMissedSequences() {
        // Create ranges with a gap between them
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        // Calculate missed sequences
        checkPoint.calculate();

        // Verify missed sequences
        assertNotNull(checkPoint.getMissedSeqs().get(TEST_KEY_1));
        assertEquals(checkPoint.getMissedSeqs().get(TEST_KEY_1).size(), 1);
        List<ConsumerCheckPoint.SeqRange> missedRange = checkPoint.getMissedSeqs().get(TEST_KEY_1);
        assertEquals(missedRange.getFirst().getStart().seq(), 5L);
        assertEquals(missedRange.getLast().getEnd().seq(), 10L);
    }

    @Test
    public void testCalculateDuplicates() {
        // Create overlapping ranges
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));

        // Calculate duplicates
        checkPoint.calculate();

        // Verify duplicates
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        // Overlap from 5 to 10 = 6 messages
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 6L);
    }

    @Test
    public void testMultipleKeys() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_2, createRangeMap("10", 10L, 15L));

        // Verify both keys were added
        assertEquals(checkPoint.getConsumed().size(), 2);
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_1));
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_2));
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_2).get("10").getEnd().seq(), 15L);
    }

    @Test
    public void testNoMissedSequencesOrDuplicates() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("6", 6L, 10L));

        // Calculate
        checkPoint.calculate();

        // Verify no missed sequences or duplicates
        assertFalse(checkPoint.getMissedSeqs().containsKey(TEST_KEY_1));
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }

    @Test
    public void testDuplicateCountWithFullyOverlap() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 5L, 8L));

        checkPoint.calculate();

        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 4L);
    }

    @Test
    public void testDuplicateCountWithMultipleOverlaps() {
        // Add multiple overlapping ranges to test more complex scenarios
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("12", 12L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("25", 18L, 22L));

        checkPoint.calculate();

        // Verify duplicates are correctly counted
        // Overlaps: 5-10 (6 messages) + 12-15 (4 messages) + 18-20 (3 messages) = 13 messages
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 13L);
    }

    @Test
    public void testDuplicateCountWithCompletelyContainedRanges() {
        // Test when one range is completely contained within another
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));

        checkPoint.calculate();

        // The range 5-10 (6 messages) is completely contained within 1-20
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 6L);
    }

    @Test
    public void testDuplicateCountWithMultipleContainedRanges() {
        // Test with multiple contained ranges
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 30L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 15L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("25", 25L, 28L));

        checkPoint.calculate();

        // Contained ranges: 5-10 (6 messages) + 15-20 (6 messages) + 25-28 (4 messages) = 16 messages
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 16L);
    }

    @Test
    public void testCalculateMultipleMissedSequences() {
        // Create ranges with multiple gaps between them
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("20", 20L, 25L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("30", 30L, 35L));

        // Calculate missed sequences
        checkPoint.calculate();

        // Verify missed sequences
        assertNotNull(checkPoint.getMissedSeqs().get(TEST_KEY_1));
        assertEquals(checkPoint.getMissedSeqs().get(TEST_KEY_1).size(), 3);

        // Get the list of missed sequences once
        List<ConsumerCheckPoint.SeqRange> missedRanges = checkPoint.getMissedSeqs().get(TEST_KEY_1);

        // First gap: 6-9
        assertEquals(missedRanges.get(0).getStart().seq(), 5L);
        assertEquals(missedRanges.get(0).getEnd().seq(), 10L);

        // Second gap: 16-19
        assertEquals(missedRanges.get(1).getStart().seq(), 15L);
        assertEquals(missedRanges.get(1).getEnd().seq(), 20L);

        // Third gap: 26-29
        assertEquals(missedRanges.get(2).getStart().seq(), 25L);
        assertEquals(missedRanges.get(2).getEnd().seq(), 30L);

        // Test adding a range that overlaps with an existing range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("35", 10L, 40L));
        checkPoint.calculate();
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        // Overlap between range [10-15] [20-25] [30-35] and [10-40] is 18 messages
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 18L);
    }

    @Test
    public void testDuplicateCountWithAdjacentRanges() {
        // Test with adjacent ranges (no overlap)
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("6", 6L, 10L));

        checkPoint.calculate();

        // Adjacent ranges should not count as duplicates
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }

    @Test
    public void testDuplicateCountDoesNotRepeatIntrinsicDuplicatesAcrossEarlierRanges() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        Map<String, ConsumerCheckPoint.SeqRange> duplicatedRange = createRangeMap("20", 20L, 25L);
        duplicatedRange.get("20").setDuplicated(3);
        checkPoint.addKey(TEST_KEY_1, duplicatedRange);

        checkPoint.calculate();

        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 3L);
    }

    @Test
    public void testDuplicateCountWithOverlapAndIntrinsicDuplicates() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        Map<String, ConsumerCheckPoint.SeqRange> duplicatedRange = createRangeMap("20", 3L, 12L);
        duplicatedRange.get("20").setDuplicated(4);
        checkPoint.addKey(TEST_KEY_1, duplicatedRange);

        checkPoint.calculate();

        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 10L);
    }

    @Test
    public void testDuplicateCountWithMultipleIntrinsicDuplicateRanges() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        Map<String, ConsumerCheckPoint.SeqRange> duplicatedRange1 = createRangeMap("20", 3L, 12L);
        duplicatedRange1.get("20").setDuplicated(4);
        checkPoint.addKey(TEST_KEY_1, duplicatedRange1);

        Map<String, ConsumerCheckPoint.SeqRange> duplicatedRange2 = createRangeMap("30", 18L, 22L);
        duplicatedRange2.get("30").setDuplicated(2);
        checkPoint.addKey(TEST_KEY_1, duplicatedRange2);

        checkPoint.calculate();

        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        // Overlaps: 3-5 (3), 10-12 (3); intrinsic duplicates: 4 and 2.
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 12L);
    }

    @Test
    public void testTrimRedundantRanges() {
        // Add all ranges to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("2", 21L, 30L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 15L, 18L));

        // Before trimming, we should have 4 ranges
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 4);

        // Calculate which will trigger the trim method
        checkPoint.calculate();

        // After trimming, we should have fewer ranges
        // Note: Since the trim method modifies the list in-place, we need to check the size
        // of the consumed map after calculation

        // The trim method should have removed the contained ranges
        // We need to verify that only the main range remains
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);

        // Verify that the remaining range is the main range
        ConsumerCheckPoint.SeqRange remainingRange = checkPoint.getConsumed().get(TEST_KEY_1).firstEntry().getValue();
        assertEquals(remainingRange.getStart().seq(), 1L);
        assertEquals(remainingRange.getEnd().seq(), 20L);

        ConsumerCheckPoint.SeqRange mergedRange = checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst();
        assertEquals(mergedRange.getStart().seq(), 1L);
        assertEquals(mergedRange.getEnd().seq(), 30L);
    }

    @Test
    public void testTrimWithNonContainedRanges() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("20", 20L, 30L));

        // Before trimming, we should have 2 ranges
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);

        // Calculate which will trigger the trim method
        checkPoint.calculate();

        // After trimming, we should still have 2 ranges since neither is contained within the other
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
    }

    @Test
    public void testEmptyCheckpoint() {
        // Verify behavior with empty checkpoint
        assertNull(checkPoint.getLastSeq(TEST_KEY_1));

        // Calculate on empty checkpoint
        checkPoint.calculate();

        // Verify no missed sequences or duplicates
        assertFalse(checkPoint.getMissedSeqs().containsKey(TEST_KEY_1));
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }

    @Test
    public void testMergeWithEmptyCheckpoint() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));

        // Create empty checkpoint
        ConsumerCheckPoint emptyCheckPoint = new ConsumerCheckPoint();

        // Merge the checkpoints
        checkPoint.merge(emptyCheckPoint);

        // Verify the merge didn't change anything
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);

        // Now merge in the other direction
        emptyCheckPoint.merge(checkPoint);

        // Verify the empty checkpoint now has the data
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
    }

    @Test
    public void testMergeOverlappingRanges() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));

        // Create second checkpoint with an overlapping range
        ConsumerCheckPoint otherCheckPoint = new ConsumerCheckPoint();
        otherCheckPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));

        // Merge the checkpoints
        checkPoint.merge(otherCheckPoint);

        // Verify the merge
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);

        // Calculate which will trigger the trim method
        checkPoint.calculate();

        // After merging and trimming, we should have a single continuous range
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getStart().seq(), 1L);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getEnd().seq(), 15L);
    }

    @Test
    public void testTrimEdgeCases() {
        // Test with empty map
        checkPoint.trim();
        assertTrue(checkPoint.getMergedConsumed().isEmpty());

        // Test with single range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.trim();

        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getStart().seq(), 1L);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getEnd().seq(), 5L);
    }

    @Test
    public void testSeqRangeMerge() {
        // Create a base range
        ConsumerCheckPoint.SeqRange range1 = new ConsumerCheckPoint.SeqRange();
        range1.setStart(new LongSeq(5, MessageMetadata.empty()));
        range1.setEnd(new LongSeq(10, MessageMetadata.empty()));

        // Case 1: Merge with overlapping range (extends end)
        ConsumerCheckPoint.SeqRange range2 = new ConsumerCheckPoint.SeqRange();
        range2.setStart(new LongSeq(8, MessageMetadata.empty()));
        range2.setEnd(new LongSeq(15, MessageMetadata.empty()));

        assertTrue(range1.merge(range2));
        assertEquals(range1.getStart().seq(), 5L);
        assertEquals(range1.getEnd().seq(), 15L);
        // Verify duplicated count: overlap is from 8 to 10 (3 values)
        assertEquals(range1.getDuplicated(), 3);

        // Reset range1 for next test
        range1.setStart(new LongSeq(5, MessageMetadata.empty()));
        range1.setEnd(new LongSeq(10, MessageMetadata.empty()));
        range1.setDuplicated(0);

        // Case 2: Merge with overlapping range (extends start)
        ConsumerCheckPoint.SeqRange range3 = new ConsumerCheckPoint.SeqRange();
        range3.setStart(new LongSeq(1, MessageMetadata.empty()));
        range3.setEnd(new LongSeq(7, MessageMetadata.empty()));

        assertTrue(range1.merge(range3));
        assertEquals(range1.getStart().seq(), 1L);
        assertEquals(range1.getEnd().seq(), 10L);
        // Verify duplicated count: overlap is from 5 to 7 (3 values)
        assertEquals(range1.getDuplicated(), 3);

        // Reset range1 for next test
        range1.setStart(new LongSeq(5, MessageMetadata.empty()));
        range1.setEnd(new LongSeq(10, MessageMetadata.empty()));
        range1.setDuplicated(0);

        // Case 3: Merge with contained range (no change to bounds)
        ConsumerCheckPoint.SeqRange range4 = new ConsumerCheckPoint.SeqRange();
        range4.setStart(new LongSeq(6, MessageMetadata.empty()));
        range4.setEnd(new LongSeq(9, MessageMetadata.empty()));

        assertTrue(range1.merge(range4));
        assertEquals(range1.getStart().seq(), 5L);
        assertEquals(range1.getEnd().seq(), 10L);
        // Verify duplicated count: overlap is from 6 to 9 (4 values)
        assertEquals(range1.getDuplicated(), 4);

        // Case 4: Merge with non-overlapping range (should fail)
        ConsumerCheckPoint.SeqRange range5 = new ConsumerCheckPoint.SeqRange();
        range5.setStart(new LongSeq(20, MessageMetadata.empty()));
        range5.setEnd(new LongSeq(25, MessageMetadata.empty()));

        assertFalse(range1.merge(range5));
        assertEquals(range1.getStart().seq(), 5L);
        assertEquals(range1.getEnd().seq(), 10L);
        // Verify duplicated count remains unchanged
        assertEquals(range1.getDuplicated(), 4);

        // Case 5: Test with ranges that have existing duplicated counts
        range1.setStart(new LongSeq(5, MessageMetadata.empty()));
        range1.setEnd(new LongSeq(10, MessageMetadata.empty()));
        range1.setDuplicated(2);

        ConsumerCheckPoint.SeqRange range6 = new ConsumerCheckPoint.SeqRange();
        range6.setStart(new LongSeq(8, MessageMetadata.empty()));
        range6.setEnd(new LongSeq(15, MessageMetadata.empty()));
        range6.setDuplicated(3);

        assertTrue(range1.merge(range6));
        assertEquals(range1.getStart().seq(), 5L);
        assertEquals(range1.getEnd().seq(), 15L);
        // Verify duplicated count: 2 (original) + 3 (from range6) + 3 (new overlap) = 8
        assertEquals(range1.getDuplicated(), 8);
    }

    @Test
    public void testGetDuplicatedCountIncludesRangeDuplicates() {
        // Create a ConsumerCheckPoint instance
        ConsumerCheckPoint checkPoint = new ConsumerCheckPoint();

        // Create a key and timestamp for our test
        String key = "testKey";
        String timestamp1 = "2023-01-01T10:00:00";
        String timestamp2 = "2023-01-01T10:01:00";

        // Create two ranges with some overlap
        ConsumerCheckPoint.SeqRange range1 = new ConsumerCheckPoint.SeqRange();
        range1.setStart(new LongSeq(1, MessageMetadata.empty()));
        range1.setEnd(new LongSeq(10, MessageMetadata.empty()));
        // Set a pre-existing duplicated count in the range
        range1.setDuplicated(5);

        Map<String, ConsumerCheckPoint.SeqRange> rangeMap = new HashMap<>();
        rangeMap.put(timestamp1, range1);
        checkPoint.addKey(key, rangeMap);
        checkPoint.calculate();

        Map<String, Long> duplicatedCount = checkPoint.getDuplicatedCount();
        assertTrue(duplicatedCount.containsKey(key));
        assertEquals(duplicatedCount.get(key).longValue(), 5L);

        ConsumerCheckPoint.SeqRange range2 = new ConsumerCheckPoint.SeqRange();
        range2.setStart(new LongSeq(8, MessageMetadata.empty()));
        range2.setEnd(new LongSeq(15, MessageMetadata.empty()));
        // Set a pre-existing duplicated count in the range
        range2.setDuplicated(3);

        // Add ranges to the checkpoint
        rangeMap.put(timestamp2, range2);
        checkPoint.addKey(key, rangeMap);

        // Calculate duplicates
        checkPoint.calculate();

        // Verify results
        duplicatedCount = checkPoint.getDuplicatedCount();
        assertTrue(duplicatedCount.containsKey(key));

        // Expected duplicates:
        // - 3 from overlap between ranges (sequences 8, 9, 10)
        // - 5 from range1's pre-existing duplicated count
        // - 3 from range2's pre-existing duplicated count
        // Total: 11
        assertEquals(duplicatedCount.get(key).longValue(), 11L);
    }

    @Test
    public void testWriteDuplicatesSeqsCalculation() {
        // 1. Add regular consumed ranges
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("3", 15L, 20L));

        // 2. Add write duplicates or out of order ranges
        Map<String, ConsumerCheckPoint.SeqRange> writeDupsMap = new HashMap<>();
        ConsumerCheckPoint.SeqRange dupRange1 = new ConsumerCheckPoint.SeqRange();
        dupRange1.setStart(new LongSeq(3L, new MessageMetadata(-1L)));
        dupRange1.setEnd(new LongSeq(5L, new MessageMetadata(-1L)));
        writeDupsMap.put("4", dupRange1);

        ConsumerCheckPoint.SeqRange dupRange2 = new ConsumerCheckPoint.SeqRange();
        dupRange2.setStart(new LongSeq(16L, new MessageMetadata(-1L)));
        dupRange2.setEnd(new LongSeq(18L, new MessageMetadata(-1L)));
        writeDupsMap.put("2", dupRange2);

        // Add a range that won't be contained in any consumed range
        ConsumerCheckPoint.SeqRange dupRange3 = new ConsumerCheckPoint.SeqRange();
        dupRange3.setStart(new LongSeq(25L, new MessageMetadata(-1L)));
        dupRange3.setEnd(new LongSeq(30L, new MessageMetadata(-1L)));
        writeDupsMap.put("5", dupRange3);

        checkPoint.addWriteDupsOrOutOrder(TEST_KEY_1, writeDupsMap);

        // Calculate to populate writeDuplicatesSeqs
        checkPoint.calculate();

        // Verify results
        Map<String, List<ConsumerCheckPoint.SeqRange>> writeDups = checkPoint.getWriteDuplicatesSeqs();

        // Should have detected duplicates for TEST_KEY_1
        assertNotNull(writeDups.get(TEST_KEY_1));

        // Should have found 2 containing ranges (for dupRange1 and dupRange2)
        assertEquals(writeDups.get(TEST_KEY_1).size(), 2);

        // Verify the duplicate ranges are correctly identified
        boolean foundFirstDupRange = false;
        boolean foundSecondDupRange = false;

        for (ConsumerCheckPoint.SeqRange range : writeDups.get(TEST_KEY_1)) {
            if (range.getStart().seq() == 3L && range.getEnd().seq() == 5L) {
                foundFirstDupRange = true;
            } else if (range.getStart().seq() == 16L && range.getEnd().seq() == 18L) {
                foundSecondDupRange = true;
            }
        }

        assertTrue(foundFirstDupRange, "Should find the duplicate range 3-5");
        assertTrue(foundSecondDupRange, "Should find the duplicate range 16-18");

        // Verify that dupRange3 (25-30) was not detected as a duplicate since it's not contained in any consumed range
        for (ConsumerCheckPoint.SeqRange range : writeDups.get(TEST_KEY_1)) {
            assertFalse(range.getStart().seq() == 25L && range.getEnd().seq() == 30L,
                    "Should not find range 25-30 as it's not contained in any consumed range");
        }
    }

    @Test
    public void testOutOfOrderTimestampRanges() {
        // Test case from the user's issue:
        // Ranges arrive in timestamp order but are out of sequence order

        // First range: [0-4607] at timestamp 2025-09-17T00:47:44.374298848
        Map<String, ConsumerCheckPoint.SeqRange> range1Map = new HashMap<>();
        ConsumerCheckPoint.SeqRange range1 = new ConsumerCheckPoint.SeqRange();
        range1.setStart(new LongSeq(0L, new MessageMetadata(19L, null, null, 11, -1L)));
        range1.setEnd(new LongSeq(4607L, new MessageMetadata(79922L, null, null, 11, -1L)));
        range1.setDuplicated(24);
        range1Map.put("2025-09-17T00:47:44.374298848", range1);

        // Second range: [4635-4635] at timestamp 2025-09-17T00:49:18.505461392
        Map<String, ConsumerCheckPoint.SeqRange> range2Map = new HashMap<>();
        ConsumerCheckPoint.SeqRange range2 = new ConsumerCheckPoint.SeqRange();
        range2.setStart(new LongSeq(4635L, new MessageMetadata(80449L, null, null, 11, -1L)));
        range2.setEnd(new LongSeq(4635L, new MessageMetadata(80449L, null, null, 11, -1L)));
        range2.setDuplicated(0);
        range2Map.put("2025-09-17T00:49:18.505461392", range2);

        // Third range: [4608-4634] at timestamp 2025-09-17T00:49:18.507581828
        Map<String, ConsumerCheckPoint.SeqRange> range3Map = new HashMap<>();
        ConsumerCheckPoint.SeqRange range3 = new ConsumerCheckPoint.SeqRange();
        range3.setStart(new LongSeq(4608L, new MessageMetadata(80686L, null, null, 11, -1L)));
        range3.setEnd(new LongSeq(4634L, new MessageMetadata(80738L, null, null, 11, -1L)));
        range3.setDuplicated(0);
        range3Map.put("2025-09-17T00:49:18.507581828", range3);

        // Add ranges to checkpoint in timestamp order (which is out of sequence order)
        checkPoint.addKey("wOz8x", range1Map);
        checkPoint.addKey("wOz8x", range2Map);
        checkPoint.addKey("wOz8x", range3Map);

        // Calculate to trigger trimming and gap detection
        checkPoint.calculate();

        // After the fix, there should be NO missed sequences since the ranges are actually continuous
        // [0-4607] + [4608-4634] + [4635-4635] = [0-4635] (continuous)
        assertFalse(checkPoint.getMissedSeqs().containsKey("wOz8x"),
            "Should not have missed sequences when ranges are actually continuous");

        // Verify the merged consumed ranges
        assertNotNull(checkPoint.getMergedConsumed().get("wOz8x"));
        assertEquals(checkPoint.getMergedConsumed().get("wOz8x").size(), 1,
            "Should have merged into a single continuous range");

        ConsumerCheckPoint.SeqRange mergedRange = checkPoint.getMergedConsumed().get("wOz8x").getFirst();
        assertEquals(mergedRange.getStart().seq(), 0L, "Merged range should start at 0");
        assertEquals(mergedRange.getEnd().seq(), 4635L, "Merged range should end at 4635");

        // Verify last sequence
        LongSeq lastSeq = checkPoint.getLastSeq("wOz8x");
        assertNotNull(lastSeq);
        assertEquals(lastSeq.seq(), 4635L, "Last sequence should be 4635");
    }

    private ConsumerCheckPoint.SeqRange range(long start, long end) {
        ConsumerCheckPoint.SeqRange r = new ConsumerCheckPoint.SeqRange();
        r.setStart(new LongSeq(start, new MessageMetadata(start)));
        r.setEnd(new LongSeq(end, new MessageMetadata(end)));
        return r;
    }

    @Test
    public void testComputeHighWatermarksContiguous() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges = new TreeMap<>();
        ranges.put("t1", range(0, 100));
        cp.addKey("key1", ranges);
        Map<String, Long> watermarks = cp.computeHighWatermarks();
        assertEquals(watermarks.get("key1").longValue(), 100L);
    }

    @Test
    public void testComputeHighWatermarksWithGap() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges = new TreeMap<>();
        ranges.put("t1", range(0, 50));
        ranges.put("t2", range(60, 100));
        cp.addKey("key1", ranges);
        Map<String, Long> watermarks = cp.computeHighWatermarks();
        assertEquals(watermarks.get("key1").longValue(), 50L);
    }

    @Test
    public void testComputeHighWatermarksNotStartingAtZero() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges = new TreeMap<>();
        ranges.put("t1", range(5, 100));
        cp.addKey("key1", ranges);
        Map<String, Long> watermarks = cp.computeHighWatermarks();
        assertEquals(watermarks.get("key1").longValue(), -1L);
    }

    @Test
    public void testComputeHighWatermarksMultipleKeys() {
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges1 = new TreeMap<>();
        ranges1.put("t1", range(0, 200));
        cp.addKey("key1", ranges1);
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges2 = new TreeMap<>();
        ranges2.put("t1", range(0, 50));
        ranges2.put("t2", range(52, 100));
        cp.addKey("key2", ranges2);
        Map<String, Long> watermarks = cp.computeHighWatermarks();
        assertEquals(watermarks.get("key1").longValue(), 200L);
        assertEquals(watermarks.get("key2").longValue(), 50L);
    }

    @Test
    public void testComputeHighWatermarksWithBaseWatermarks() {
        // Simulates second checkpoint cycle after watermark trimming:
        // ranges no longer start at 0 because workers trimmed below watermark 50
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges = new TreeMap<>();
        ranges.put("t1", range(51, 200));
        cp.addKey("key1", ranges);

        // Without base: returns -1 (doesn't start at 0)
        Map<String, Long> withoutBase = cp.computeHighWatermarks();
        assertEquals(withoutBase.get("key1").longValue(), -1L);

        // With base watermark of 50: contiguous from 50, advances to 200
        Map<String, Long> withBase = cp.computeHighWatermarks(Map.of("key1", 50L));
        assertEquals(withBase.get("key1").longValue(), 200L);
    }

    @Test
    public void testComputeHighWatermarksWithBaseAndGap() {
        // Ranges start above base watermark but with a gap
        ConsumerCheckPoint cp = new ConsumerCheckPoint();
        TreeMap<String, ConsumerCheckPoint.SeqRange> ranges = new TreeMap<>();
        ranges.put("t1", range(55, 200));  // gap: 51-54 missing
        cp.addKey("key1", ranges);

        // Base is 50, first range starts at 55 — gap, watermark stays at 50
        Map<String, Long> watermarks = cp.computeHighWatermarks(Map.of("key1", 50L));
        assertEquals(watermarks.get("key1").longValue(), 50L);
    }

    @Test
    public void testComputeHighWatermarksWithBaseEmptyRanges() {
        // Key exists in base but has no ranges (all trimmed, no new messages)
        ConsumerCheckPoint cp = new ConsumerCheckPoint();

        Map<String, Long> watermarks = cp.computeHighWatermarks(Map.of("key1", 100L));
        // key1 not in mergedConsumed, base watermark is preserved
        assertEquals(watermarks.get("key1").longValue(), 100L);
    }

    private Map<String, ConsumerCheckPoint.SeqRange> createRangeMap(String orderKey, Long start, Long end) {
        Map<String, ConsumerCheckPoint.SeqRange> rangeMap = new HashMap<>();
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(new LongSeq(start, new MessageMetadata(start)));
        range.setEnd(new LongSeq(end, new MessageMetadata(end)));
        rangeMap.put(orderKey, range);
        return rangeMap;
    }
}
