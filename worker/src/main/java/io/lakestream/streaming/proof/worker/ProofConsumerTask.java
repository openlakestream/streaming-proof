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
package io.lakestream.streaming.proof.worker;

import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.ProofValue;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A task that consumes messages and tracks sequence ranges for validation.
 * This class is responsible for:
 * - Consuming messages from a streaming system
 * - Tracking sequence numbers for each message key
 * - Maintaining sequence ranges for consumed messages
 */
@Slf4j
public class ProofConsumerTask implements MessageListener, AutoCloseable {

    /** Whether this task operates in shared subscription mode */
    @Getter
    private final boolean sharedMode;

    /** The consumer instance that this task is associated with */
    @Setter
    private ProofConsumer consumer;

    public ProofConsumerTask() {
        this(false);
    }

    public ProofConsumerTask(boolean sharedMode) {
        this(sharedMode, ProofValue.MIN_SIZE);
    }

    public ProofConsumerTask(boolean sharedMode, int messageSize) {
        this.sharedMode = sharedMode;
        this.messageSize = messageSize;
    }

    /** Total size in bytes of each message value, used for throughput accounting */
    private final int messageSize;

    /**
     * Map storing consumed sequence ranges for each key.
     * The outer map's key is the message key.
     * The inner map's key is the timestamp as a formatted date/time string.
     */
    @Getter
    private final Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> consumed = new HashMap<>();

    /**
     * Map storing sequence ranges for messages that are either duplicated at the producer side
     * or received out of order (with a lower sequence number but higher offset or message ID).
     * The outer map's key is the message key.
     * The inner map's key is the timestamp as a formatted date/time string.
     */
    @Getter
    private final Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> writeDupsOrOutOrder = new HashMap<>();

    @Getter
    private final AtomicLong receivedMessages = new AtomicLong(0);

    @Getter
    private final AtomicLong receivedBytes = new AtomicLong(0);

    private final LatencyRecorder endToEndLatency = new LatencyRecorder();

    /** End-to-end latency samples for windowed metrics (reset on each snapshot read) */
    private final LatencyRecorder endToEndLatencyWindow = new LatencyRecorder();

    /**
     * Processes a received message and validates its sequence number against the expected order.
     * This method is synchronized to ensure thread-safe updates to the sequence tracking maps.
     *
     * @param key The message key used for sequence tracking and message grouping
     * @param value The sequence number of the message, used to verify ordering
     * @param metadata Additional message information such as offset or timestamp
     * @see LongSeq
     * @see MessageMetadata
     */
    @Override
    public synchronized void onMessage(String key, long value, MessageMetadata metadata) {
        receivedMessages.incrementAndGet();
        receivedBytes.addAndGet(estimateMessageBytes(key));
        if (metadata != null && metadata.publishTimestampMillis() != null && metadata.publishTimestampMillis() > 0) {
            long latencyMillis = System.currentTimeMillis() - metadata.publishTimestampMillis();
            endToEndLatency.record(latencyMillis);
            endToEndLatencyWindow.record(latencyMillis);
        }
        LongSeq newMsg = new LongSeq(value, metadata);
        ConsumerCheckPoint.SeqRange lastConsumedRange = getLastSeq(key);
        if (lastConsumedRange == null) {
            newConsumedRange(key, newMsg);
            return;
        }

        if (sharedMode) {
            LongSeq lastConsumedSeq = lastConsumedRange.getEnd();
            if (value == lastConsumedSeq.seq() + 1) {
                lastConsumedRange.setEnd(newMsg);
            } else {
                newConsumedRange(key, newMsg);
            }
            return;
        }

        LongSeq lastConsumedSeq = lastConsumedRange.getEnd();

        if (value <= lastConsumedSeq.seq()) {
            if (metadata.isAfter(lastConsumedSeq.metadata())) {
                // Handle duplicated writes from the producer side
                ConsumerCheckPoint.SeqRange lastSeqFromProducerDups = getLastWriteDupOrOutOrder(key);
                if (lastSeqFromProducerDups == null || value - lastSeqFromProducerDups.getEnd().seq() > 1) {
                    newWriteDupsOrOutOrderRange(key, newMsg);
                } else {
                    lastSeqFromProducerDups.setEnd(newMsg);
                }
                newConsumedRange(key, newMsg);
            } else {
                // Handle message redeliveries to the consumer side
                long dups = lastConsumedSeq.seq() - value + 1;
                log.info("[{}] Duplicated message detected | key: {} | new message: {} | last seq range: {} | dups: {}",
                        consumer.name(), key, newMsg, getLastSeq(key), dups);
                newConsumedRange(key, newMsg);
            }
        } else if (value - lastConsumedSeq.seq() > 1) {
            log.info("[{}] Range gap detected | key: {} | new message: {} | last seq range: {}",
                    consumer.name(), key, newMsg, getLastSeq(key));
            newConsumedRange(key, newMsg);
        } else {
            lastConsumedRange.setEnd(newMsg);
        }
    }

    /**
     * Trims consumed and writeDupsOrOutOrder ranges based on high watermarks.
     * Ranges fully below the watermark are removed. Ranges spanning the watermark
     * are trimmed so they start at watermark + 1. This bounds memory usage for
     * long-running proofs.
     *
     * @param watermarks A map of key to high watermark sequence number. Sequences
     *                   at or below the watermark are considered verified and can
     *                   be discarded.
     */
    public synchronized void applyHighWatermarks(Map<String, Long> watermarks) {
        watermarks.forEach((key, watermark) -> {
            SortedMap<String, ConsumerCheckPoint.SeqRange> ranges = consumed.get(key);
            if (ranges == null) {
                return;
            }
            List<String> toRemove = new ArrayList<>();
            for (Map.Entry<String, ConsumerCheckPoint.SeqRange> entry : ranges.entrySet()) {
                ConsumerCheckPoint.SeqRange range = entry.getValue();
                if (range.getEnd().seq() <= watermark) {
                    toRemove.add(entry.getKey());
                } else if (range.getStart().seq() <= watermark) {
                    range.setStart(new LongSeq(watermark + 1, range.getStart().metadata()));
                }
            }
            toRemove.forEach(ranges::remove);
            if (ranges.isEmpty()) {
                consumed.remove(key);
            }

            SortedMap<String, ConsumerCheckPoint.SeqRange> dupsRanges = writeDupsOrOutOrder.get(key);
            if (dupsRanges != null) {
                List<String> dupsToRemove = new ArrayList<>();
                for (Map.Entry<String, ConsumerCheckPoint.SeqRange> entry : dupsRanges.entrySet()) {
                    ConsumerCheckPoint.SeqRange range = entry.getValue();
                    if (range.getEnd().seq() <= watermark) {
                        dupsToRemove.add(entry.getKey());
                    } else if (range.getStart().seq() <= watermark) {
                        range.setStart(new LongSeq(watermark + 1, range.getStart().metadata()));
                    }
                }
                dupsToRemove.forEach(dupsRanges::remove);
                if (dupsRanges.isEmpty()) {
                    writeDupsOrOutOrder.remove(key);
                }
            }
        });
    }

    /**
     * Returns a trimmed version of the consumed sequence ranges by merging adjacent ranges.
     * This method processes the raw consumed sequence ranges and combines adjacent ranges
     * that can be merged, resulting in a more compact representation of the consumed data.
     *
     * The method is synchronized to ensure thread-safety when accessing shared data structures.
     *
     * @return A map containing the trimmed sequence ranges for each key. The outer map uses
     *         the message key as its key, while the inner SortedMap uses timestamps as keys
     *         and merged sequence ranges as values.
     */
    public synchronized Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> getTrimmedConsumed() {
        Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> merged = new HashMap<>();
        consumed.forEach((k, v) -> {
            SortedMap<String, ConsumerCheckPoint.SeqRange> mergedRange = new TreeMap<>();
            ConsumerCheckPoint.SeqRange range = null;
            for (Map.Entry<String, ConsumerCheckPoint.SeqRange> entry : v.entrySet()) {
                ConsumerCheckPoint.SeqRange clone = entry.getValue().clone();
                if (range == null || !range.merge(clone)) {
                    range = clone;
                    mergedRange.put(entry.getKey(), range);
                }
            }
            merged.put(k, mergedRange);
        });
        return merged;
    }

    /**
     * Retrieves the last sequence range for a given key.
     *
     * @param key The message key to look up
     * @return The last sequence range for the key, or null if the key doesn't exist or has no ranges
     */
    private ConsumerCheckPoint.SeqRange getLastSeq(String key) {
        if (!consumed.containsKey(key)) {
            return null;
        }
        if (consumed.get(key).isEmpty()) {
            return null;
        }
        return consumed.get(key).lastEntry().getValue();
    }

    private ConsumerCheckPoint.SeqRange getLastWriteDupOrOutOrder(String key) {
        if (!writeDupsOrOutOrder.containsKey(key)) {
            return null;
        }
        if (writeDupsOrOutOrder.get(key).isEmpty()) {
            return null;
        }
        return writeDupsOrOutOrder.get(key).lastEntry().getValue();
    }

    private int estimateMessageBytes(String key) {
        return (key == null ? 0 : key.getBytes(StandardCharsets.UTF_8).length) + messageSize;
    }

    /**
     * Creates a new sequence range for a key starting with the given message.
     * This is called when:
     * - A new key is encountered
     * - A duplicate message is detected
     * - A gap in the sequence is detected
     *
     * @param key The message key
     * @param newMsg The message to start the new range with
     */
    private void newConsumedRange(String key, LongSeq newMsg) {
        log.info("[{}] New consumed range | key: {} | new message: {} | last seq range: {}",
                consumer.name(), key, newMsg, getLastSeq(key));
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(newMsg);
        range.setEnd(newMsg);
        consumed.compute(key, (k, v) -> {
            if (v == null) {
                v = new TreeMap<>();
            }
            // Format the current timestamp as a readable date/time string
            String timestamp = java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
                    .format(java.time.LocalDateTime.now());
            ConsumerCheckPoint.putRange(v, timestamp, range);
            return v;
        });
    }

    /**
     * Creates a new sequence range which duplicated from the producer side or out-of-order messages.
     * This is called when:
     * - A duplicate message is detected but with a higher offset or message ID
     * - A message is received out of order (with a lower sequence number but higher offset or message ID)
     *
     * @param key The message key
     * @param newMsg The message to start the new range with
     */
    private void newWriteDupsOrOutOrderRange(String key, LongSeq newMsg) {
        log.info("[{}] New write dups or out of order range | key: {} | new message: {} | last seq range: {}",
                consumer.name(), key, newMsg, getLastSeq(key));
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(newMsg);
        range.setEnd(newMsg);
        writeDupsOrOutOrder.compute(key, (k, v) -> {
            if (v == null) {
                v = new TreeMap<>();    
            }
            // Format the current timestamp as a readable date/time string
            String timestamp = java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
                   .format(java.time.LocalDateTime.now());
            ConsumerCheckPoint.putRange(v, timestamp, range);
            return v;
        });
    }

    /**
     * Gets the name of the associated consumer.
     *
     * @return The consumer name
     */
    public String getConsumerName() {
        return consumer.name();
    }

    public LatencyMetricSnapshot getEndToEndLatencySnapshot() {
        return endToEndLatency.snapshot();
    }

    /**
     * Returns the windowed end-to-end latency snapshot and resets the window recorder.
     * Each call only reflects samples recorded since the previous call.
     */
    public LatencyMetricSnapshot getEndToEndLatencyWindowSnapshot() {
        return endToEndLatencyWindow.snapshotAndReset();
    }

    /**
     * Closes the associated consumer and releases resources.
     *
     * @throws Exception if an error occurs while closing the consumer
     */
    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
