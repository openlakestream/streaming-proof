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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.Util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Tracks and manages sequence ranges for consumed messages to verify messaging system guarantees.
 * 
 * <p>This class is the core verification component that:
 * <ul>
 *   <li>Maintains timestamp-based sequence ranges for each message key</li>
 *   <li>Detects missed messages through sequence gaps</li>
 *   <li>Identifies duplicated messages through overlapping sequence ranges</li>
 *   <li>Tracks out-of-order message delivery</li>
 *   <li>Handles partition reassignment through timestamp-based range tracking</li>
 *   <li>Optimizes storage through range merging and trimming</li>
 * </ul>
 * 
 * <p>The checkpoint uses a sophisticated range-based approach that efficiently tracks
 * large volumes of messages while providing precise verification of delivery guarantees.
 * When the Coordinator aggregates checkpoints from multiple consumers, it can detect
 * system-wide issues like message loss or duplication.
 * 
 * @see io.lakestream.streaming.proof.common.MessageListener
 * @see io.lakestream.streaming.proof.common.ProofConsumer
 */
@Data
public class ConsumerCheckPoint {

    /**
     * Represents a range of sequence numbers for tracking message consumption.
     * 
     * <p>This class efficiently tracks contiguous ranges of message sequences by storing:
     * <ul>
     *   <li>Start sequence - The first sequence number in the range</li>
     *   <li>End sequence - The last sequence number in the range</li>
     *   <li>Duplicate count - Number of duplicate messages detected within the range</li>
     *   <li>Metadata - System-specific information for tracing and debugging</li>
     * </ul>
     * 
     * <p>Sequence ranges enable efficient verification of large message volumes by:
     * <ul>
     *   <li>Compactly representing contiguous sequences</li>
     *   <li>Allowing gap detection between ranges</li>
     *   <li>Supporting range merging for optimization</li>
     *   <li>Tracking duplicates through range overlaps</li>
     * </ul>
     */
    @Data
    public static class SeqRange {
        private LongSeq start;
        private LongSeq end;
        private int duplicated;

        /**
         * Checks if this range overlaps with another range.
         *
         * @param other The other range to check
         * @return True if there's an overlap, false otherwise
         */
        private boolean hasOverlap(SeqRange other) {
            return other.getStart().seq() <= this.getEnd().seq()
                    && other.getEnd().seq() >= this.getStart().seq();
        }

        public boolean contains(SeqRange other) {
            return this.getStart().seq() <= other.getStart().seq()
                    && this.getEnd().seq() >= other.getEnd().seq();
        }

        /**
         * Merges this range with another range if they overlap.
         * If they overlap, the start and end sequence numbers will be adjusted
         * to encompass the entire range, and the duplicated count will be updated.
         *
         * @param other The other range to merge with
         * @return True if the ranges were merged, false otherwise
         */ 
        public boolean merge(SeqRange other) {
            if (hasOverlap(other)) {
                long overlapStart = Math.max(this.getStart().seq(), other.getStart().seq());
                long overlapEnd = Math.min(this.getEnd().seq(), other.getEnd().seq());
                int overlapCount = (int) (overlapEnd - overlapStart + 1);
                this.duplicated += overlapCount + other.getDuplicated();
                // Set the new boundaries
                this.start = this.start.compareTo(other.getStart()) < 0 ? this.getStart() : other.getStart();
                this.end = this.end.compareTo(other.getEnd()) > 0 ? this.getEnd() : other.getEnd();                
                return true;
            } else {
                return false;
            }
        }

        /**
         * Creates a deep copy of this SeqRange instance.
         * The clone will have the same start and end sequence numbers,
         * as well as the same duplicated count as the original.
         *
         * @return A new SeqRange instance with the same properties as this one
         */
        public SeqRange clone() {
            SeqRange clone = new SeqRange();
            clone.setStart(this.start);
            clone.setEnd(this.end);
            clone.setDuplicated(this.duplicated);
            return clone;
        }

        @Override
        public String toString() {
            try {
                return Util.JSON_WRITER.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** 
     * Map storing consumed sequence ranges for each key.
     * The outer map's key is the message key.
     * The inner map's key is the formatted date/time string representing when the range was created.
     */
    private final Map<String, SortedMap<String, SeqRange>> consumed = new HashMap<>();

    /**
     * Map storing sequence ranges for messages that are either duplicated at the producer side
     * or received out of order (with a lower sequence number but higher offset or message ID).
     * The outer map's key is the message key.
     * The inner map's key is the timestamp as a formatted date/time string.
     */
    private final Map<String, SortedMap<String, SeqRange>> writeDupsOrOutOrder = new HashMap<>();

    /**
     * Map storing optimized and merged sequence ranges for each key.
     * This is a transient field that is recalculated during processing.
     * It contains the same sequence data as the 'consumed' map but with
     * adjacent and overlapping ranges merged together for more efficient
     * gap detection and sequence analysis.
     */
    @JsonIgnore
    private final Map<String, List<SeqRange>> mergedConsumed = new HashMap<>();

    /** Map storing missed sequence ranges for each key */
    private volatile Map<String, List<SeqRange>> missedSeqs = Collections.emptyMap();
    /** Map storing the count of duplicated messages for each key */
    private volatile Map<String, Long> duplicatedCount = Collections.emptyMap();
    /** Map storing out-of-order sequence ranges for each key */
    private volatile Map<String, List<Pair<Long, Long>>> outOfOrderSeqs = Collections.emptyMap();
    /** Map storing write duplicates sequence ranges from producer for each key */
    private volatile Map<String, List<SeqRange>> writeDuplicatesSeqs = Collections.emptyMap();

    /**
     * Adds sequence ranges for a specific key.
     * If a new range connects with an existing range (end of existing + 1 = start of new),
     * they will be merged into a single range.
     *
     * @param key The message key
     * @param consumedRange Map of sequence ranges to add for the key
     */
    public void addKey(String key, Map<String, SeqRange> consumedRange) {
        SortedMap<String, SeqRange> ranges = consumed.computeIfAbsent(key, k -> new TreeMap<>());
        consumedRange.forEach((timestamp, range) -> putRange(ranges, timestamp, range));
    }

    public void addWriteDupsOrOutOrder(String key, Map<String, SeqRange> consumedRange) {
        SortedMap<String, SeqRange> ranges = writeDupsOrOutOrder.computeIfAbsent(key, k -> new TreeMap<>());
        consumedRange.forEach((timestamp, range) -> putRange(ranges, timestamp, range));
    }

    /**
     * Puts a range under the time it was created without replacing another range created at the same
     * time: the consumers of a Shared subscription often create ranges of one key within one clock tick.
     * Putting the same range again leaves a single copy.
     *
     * @param ranges The ranges of a key, by creation time
     * @param timestamp The creation time of the range
     * @param range The range to put
     */
    public static void putRange(SortedMap<String, SeqRange> ranges, String timestamp, SeqRange range) {
        // "'" sorts right after the timestamp and before any later one
        while (ranges.containsKey(timestamp) && !ranges.get(timestamp).equals(range)) {
            timestamp += "'";
        }
        ranges.put(timestamp, range);
    }

    /**
     * Merges another ConsumerCheckPoint into this one.
     * All sequence ranges from the other checkpoint will be added to this one.
     *
     * @param checkPoint The checkpoint to merge into this one
     */
    public void merge(ConsumerCheckPoint checkPoint) {
        checkPoint.consumed.forEach(this::addKey);
        checkPoint.writeDupsOrOutOrder.forEach(this::addWriteDupsOrOutOrder);
    }

    /**
     * Gets the last (highest) sequence number for a specific key.
     *
     * @param key The message key
     * @return The last sequence number, or null if the key doesn't exist or has no sequences
     */
    public LongSeq getLastSeq(String key) {
        trim();
        if (!mergedConsumed.containsKey(key)) {
            return null;
        }
        if (mergedConsumed.get(key).isEmpty()) {
            return null;
        }
        SeqRange range = mergedConsumed.get(key).getLast();
        return range.getEnd();
    }

    /**
     * Calculates missed sequences and duplicated messages based on the current state.
     * Updates the missedSeqs and duplicatedCount maps.
     */
    public void calculate() {
        Map<String, List<SeqRange>> missedSeqs = new HashMap<>();
        Map<String, Long> duplicatedCount = new HashMap<>();
        Map<String, List<SeqRange>> writeDups = new HashMap<>();
        trim();
        consumed.forEach((key, value) -> {
            List<SeqRange> values = value.values().stream().toList();
            long count = getDuplicatedCount(values);
            if (count > 0) {
                duplicatedCount.put(key, count);
            }
            
            List<SeqRange> missed = getMissedSeqs(mergedConsumed.get(key));
            if (!missed.isEmpty()) {
                missedSeqs.put(key, missed);
            }
        });
        this.missedSeqs = missedSeqs;
        this.duplicatedCount = duplicatedCount;
        this.writeDupsOrOutOrder.forEach((key, value) -> {
            value.forEach((ts, range) -> {
                if (mergedConsumed.containsKey(key)) {
                    mergedConsumed.get(key).forEach(r -> {
                        if (r.contains(range)) {
                            writeDups.compute(key, (k, v) -> {
                                if (v == null) {
                                    v = new ArrayList<>();
                                }
                                v.add(range);
                                return v;
                            });
                        }
                    });
                }
            });
        });
        this.writeDuplicatesSeqs = writeDups;
    }

    /**
     * Calculates the number of duplicated messages in the given sequence ranges.
     * A message is considered duplicated if its sequence number appears in more than one range.
     * 
     * The algorithm works by:
     * 1. Comparing each range with all previously processed ranges
     * 2. Identifying overlapping sections between ranges
     * 3. Calculating the exact number of sequence numbers that appear in multiple ranges
     * 4. Summing up all overlapping sequence numbers
     * 
     * For example, if range [1-10] overlaps with range [5-15], the duplicated count is 6
     * (representing sequence numbers 5, 6, 7, 8, 9, 10).
     *
     * @param ranges List of sequence ranges to check for duplicates
     * @return The count of duplicated messages, or 0 if no duplicates are found
     */
    private long getDuplicatedCount(List<SeqRange> ranges) {
        if (ranges.isEmpty()) {
            return 0;
        }
        long duplicates = 0;
        duplicates += ranges.getFirst().getDuplicated();
        // Iterate through consecutive ranges to find duplicates
        for (int i = 1; i < ranges.size(); i++) {
            SeqRange current = ranges.get(i);
            // trim() is only used to collapse earlier overlap windows for overlap counting.
            // Intrinsic duplicate counts stay on the original range and must be added exactly once here.
            List<SeqRange> trimmed = trim(ranges.subList(0, i));
            for (SeqRange previous : trimmed) {
                // If current start sequence is less than or equal to previous end sequence,
                // we have a duplicate or overlap
                if (current.getStart().seq() <= previous.getEnd().seq()) {
                    // Calculate how many messages are duplicated
                    long overlapStart = Math.max(current.getStart().seq(), previous.getStart().seq());
                    long overlapEnd = Math.min(current.getEnd().seq(), previous.getEnd().seq());
                    long overlap = overlapEnd - overlapStart + 1;

                    // Add to total duplicates if there's an actual overlap
                    if (overlap > 0) {
                        duplicates += overlap;
                    }
                }
            }
            duplicates += current.getDuplicated();
        }
        
        return duplicates;
    }

    /**
     * Identifies missed sequences (gaps) between the given sequence ranges.
     * A sequence is considered missed if there's a gap between consecutive ranges.
     * 
     * The algorithm works by:
     * 1. Sorting the sequence ranges in ascending order
     * 2. Comparing each range with the previous range
     * 3. Identifying gaps between the end of the previous range and the start of the current range
     * 4. Creating pairs representing the start and end of each gap
     * 
     * For example, if we have ranges [1-5] and [10-15], the missed sequence range is [6-9]
     * (representing sequence numbers 6, 7, 8, 9).
     *
     * @param ranges List of sequence ranges to check for gaps
     * @return List of pairs representing missed sequence ranges (start, end), or empty list if no gaps are found
     */
    private List<SeqRange> getMissedSeqs(List<SeqRange> ranges) {
        
        // If there's only one range or less, we can't determine missed sequences
        if (ranges.size() <= 1) {
            return Collections.emptyList();
        }
        
        List<SeqRange> missedRanges = new ArrayList<>();

        // Iterate through consecutive ranges to find gaps
        for (int i = 1; i < ranges.size(); i++) {
            SeqRange current = ranges.get(i);
            SeqRange previous = ranges.get(i - 1);
            
            // If there's a gap between the previous range's end and current range's start
            if (current.getStart().seq() > previous.getEnd().seq() + 1) {
                SeqRange missedRange = new SeqRange();
                missedRange.setStart(previous.getEnd());
                missedRange.setEnd(current.getStart());
                missedRanges.add(missedRange);
            }
        }
        
        return missedRanges;
    }

    /**
     * Computes the high-watermark for each key by walking the merged, sorted ranges.
     * The high-watermark is the highest contiguous sequence number starting from seq 0.
     * If no range starts at 0, the watermark is -1.
     *
     * @return A map from key to its high-watermark value
     */
    public Map<String, Long> computeHighWatermarks() {
        return computeHighWatermarks(Collections.emptyMap());
    }

    /**
     * Computes high watermarks using base watermarks as the starting point.
     * After watermark trimming, ranges no longer start at seq 0,
     * so the base watermark is used as the contiguity starting point.
     *
     * @param baseWatermarks Previous watermarks per key, must not be null
     * @return A map from key to its updated high-watermark value
     */
    public Map<String, Long> computeHighWatermarks(Map<String, Long> baseWatermarks) {
        java.util.Objects.requireNonNull(baseWatermarks, "baseWatermarks must not be null");
        trim();
        Map<String, Long> watermarks = new HashMap<>(baseWatermarks);
        mergedConsumed.forEach((key, ranges) -> {
            long base = baseWatermarks.getOrDefault(key, -1L);
            if (ranges.isEmpty()) {
                watermarks.put(key, base);
                return;
            }
            // Check if the first range is contiguous with the base watermark
            long firstStart = ranges.getFirst().getStart().seq();
            if (base == -1L && firstStart != 0) {
                // No base and doesn't start at 0
                watermarks.put(key, -1L);
                return;
            }
            if (base >= 0 && firstStart > base + 1) {
                // Gap between base watermark and first range
                watermarks.put(key, base);
                return;
            }
            long watermark = Math.max(base, ranges.getFirst().getEnd().seq());
            for (int i = 1; i < ranges.size(); i++) {
                SeqRange next = ranges.get(i);
                if (next.getStart().seq() <= watermark + 1) {
                    watermark = Math.max(watermark, next.getEnd().seq());
                } else {
                    break;
                }
            }
            watermarks.put(key, watermark);
        });
        return watermarks;
    }

    /**
     * Processes the consumed map to create optimized sequence ranges in the mergedConsumed map.
     * This method performs two key optimizations:
     * 1. Merges adjacent or overlapping ranges into single continuous ranges
     * 2. Eliminates redundant ranges that are completely contained within other ranges
     * 
     * The resulting mergedConsumed map contains a more efficient representation
     * of the sequence data, which improves performance for gap detection and
     * other sequence analysis operations.
     */
    public void trim() {
        mergedConsumed.clear();
        consumed.forEach((key, rangeMap) -> {
            if (rangeMap.isEmpty()) {
                return;
            }
            mergedConsumed.put(key, trim(rangeMap.values().stream().toList()));
        });
    }

    private List<SeqRange> trim(List<SeqRange> ranges) {
        if (ranges.size() <= 1) {
            return ranges;
        }

        // Sort ranges by start sequence number to ensure proper merging
        List<SeqRange> sortedRanges = ranges.stream()
            .sorted(java.util.Comparator.comparingLong(r -> r.getStart().seq()))
            .toList();

        List<SeqRange> mergedRanges = new ArrayList<>();
        // Start with the first range
        SeqRange currentRange = new SeqRange();
        currentRange.setStart(sortedRanges.getFirst().getStart());
        currentRange.setEnd(sortedRanges.getFirst().getEnd());

        // Try to merge subsequent ranges
        for (int i = 1; i < sortedRanges.size(); i++) {
            SeqRange nextRange = sortedRanges.get(i);
            long currentEnd = currentRange.getEnd().seq();
            long nextStart = nextRange.getStart().seq();
            long nextEnd = nextRange.getEnd().seq();

            // Case 1: Overlap - next range starts before or at current range end
            if (nextStart <= currentEnd) {
                // Extend the current range if the next range ends after the current range
                if (nextEnd > currentEnd) {
                    currentRange.setEnd(nextRange.getEnd());
                }
                // If next range is completely contained in current range, do nothing
            } else if (nextStart == currentEnd + 1) {
                // Case 2: No gap, no overlap - next range starts exactly one after current range end
                // Simply extend the current range
                currentRange.setEnd(nextRange.getEnd());
            } else {
                // Case 3: Gap - next range starts more than one after current range end
                // Add the current range to the result and start a new one
                mergedRanges.add(currentRange);
                currentRange = new SeqRange();
                currentRange.setStart(nextRange.getStart());
                currentRange.setEnd(nextRange.getEnd());
            }
        }

        // Add the last range
        mergedRanges.add(currentRange);
        return mergedRanges;
    }
}
