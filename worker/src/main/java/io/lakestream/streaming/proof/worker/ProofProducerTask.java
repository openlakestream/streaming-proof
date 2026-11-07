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
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import io.lakestream.streaming.proof.common.records.ErrorOccurrence;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * A task that manages message production for streaming proof tests. This class handles
 * the generation and sending of messages with sequential values for a set of unique keys.
 * It implements {@link AutoCloseable} for proper resource cleanup.
 *
 * <p>Key features:
 * <ul>
 *   <li>Round-robin message distribution across keys</li>
 *   <li>Sequential value tracking per key</li>
 *   <li>Asynchronous message production</li>
 *   <li>Error tracking and failed sequence recording</li>
 * </ul>
 *
 * <p>The task maintains:
 * <ul>
 *   <li>A fixed set of unique message keys</li>
 *   <li>Sequence counters for each key</li>
 *   <li>Aggregated error occurrence details for failed sends</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofProducer producer = driver.createProducer("test-topic", configs);
 * ProofProducerTask task = new ProofProducerTask(producer, 10); // 10 unique keys
 *
 * // Send messages asynchronously
 * task.sendAsync()
 *     .thenAccept(v -> System.out.println("Message sent successfully"))
 *     .exceptionally(e -> {
 *         System.err.println("Send failed: " + e);
 *         return null;
 *     });
 *
 * // Later, check the metrics
 * System.out.println("Errors: " + task.getErrorDetails());
 * }</pre>
 *
 * @see ProofProducer
 * @see ProofProducers
 */
@Slf4j
public class ProofProducerTask implements AutoCloseable {

    /** The underlying producer for sending messages */
    private final ProofProducer producer;

    /** Number of unique keys managed by this task */
    private final int keys;

    /** Total size in bytes of each message value, used for throughput accounting */
    private final int messageSize;

    /** Array of unique keys for round-robin message distribution */
    private final String[] keyArray;

    /** Maps each key to its current sequence number */
    private final Map<String, AtomicLong> keySeq;

    /** Contiguous successful acknowledgement watermarks, also used as the state lock. */
    private final Map<String, LongSeq> lastPublished;

    /** Successful completions waiting for earlier sequences, guarded by lastPublished. */
    private final Map<String, NavigableMap<Long, LongSeq>> completed = new HashMap<>();

    /** Earliest terminal failure per key; a checkpoint must never cross it. */
    private final Map<String, Long> firstFailed = new HashMap<>();

    /** Last acknowledgement with a known position, guarded by lastPublished. */
    private final Map<String, LongSeq> lastPositioned = new HashMap<>();

    /** Aggregated timing details for failed message sends */
    private final Map<String, ErrorOccurrence> errorDetails = new HashMap<>();

    /** Counter for round-robin key selection */
    private final AtomicLong index = new AtomicLong(0);

    /** Total send attempts */
    private final AtomicLong sendAttempts = new AtomicLong(0);

    /** Terminal driver send failures, excluding acknowledgement validation errors. */
    private final AtomicLong sendFailures = new AtomicLong(0);

    /** Total acknowledged sends */
    private final AtomicLong acknowledged = new AtomicLong(0);

    /** Estimated logical bytes acknowledged by the producer */
    private final AtomicLong acknowledgedBytes = new AtomicLong(0);

    /** Publish latency samples (cumulative, never reset) */
    private final LatencyRecorder publishLatency = new LatencyRecorder();

    /** Publish latency samples for windowed metrics (reset on each snapshot read) */
    private final LatencyRecorder publishLatencyWindow = new LatencyRecorder();
    private final FixedWindowLatencyRecorder fixedPublishLatencyWindows;

    /**
     * Creates a new producer task with the specified number of unique keys.
     *
     * @param producer The underlying producer to use for sending messages
     * @param keys The number of unique keys to manage
     * @param messageSize The total size in bytes of each message value, used for
     *                    throughput accounting
     */
    public ProofProducerTask(ProofProducer producer, int keys, int messageSize) {
        this(producer, keys, messageSize, null);
    }

    ProofProducerTask(ProofProducer producer, int keys, int messageSize,
                      FixedWindowLatencyRecorder fixedPublishLatencyWindows) {
        this.fixedPublishLatencyWindows = fixedPublishLatencyWindows;
        if (keys <= 0) {
            throw new IllegalArgumentException("Producer tasks require at least one key");
        }
        this.producer = producer;
        this.keys = keys;
        this.messageSize = messageSize;
        this.keyArray = new String[keys];
        Map<String, AtomicLong> map = new HashMap<>();
        Map<String, LongSeq> last = new HashMap<>();
        for (int i = 0; i < keys; i++) {
            String key = RandomStringUtils.secure().nextAlphanumeric(5);
            map.put(key, new AtomicLong(0));
            last.put(key, LongSeq.empty());
            keyArray[i] = key;
        }
        this.keySeq = Collections.unmodifiableMap(map);
        this.lastPublished = last;
    }

    /**
     * Asynchronously sends a message with the next sequence number for a key.
     * Keys are selected in round-robin fashion, and sequence numbers are
     * incremented atomically for each key.
     *
     * @return A future that completes when the send operation is done
     */
    public CompletableFuture<MessageMetadata> sendAsync() {
        String key = keyArray[(int) (index.getAndIncrement() % keys)];
        long seq = keySeq.get(key).getAndIncrement();
        long startedAtNanos = System.nanoTime();
        sendAttempts.incrementAndGet();
        CompletableFuture<MessageMetadata> send;
        try {
            send = Objects.requireNonNull(producer.sendAsync(key, seq), "Producer returned a null future");
        } catch (RuntimeException e) {
            recordSendFailure(key, seq, e);
            return CompletableFuture.failedFuture(e);
        }
        return send.whenComplete((metadata, e) -> {
            if (e != null) {
                recordSendFailure(key, seq, e);
                return;
            }
            // Count transport acknowledgements separately from validation errors.
            acknowledged.incrementAndGet();
            long processingSeq = seq;
            try {
                Objects.requireNonNull(metadata, "Producer returned null acknowledgement metadata");
                acknowledgedBytes.addAndGet(estimateMessageBytes(key));
                long latencyMillis = (System.nanoTime() - startedAtNanos) / 1_000_000L;
                publishLatency.record(latencyMillis);
                publishLatencyWindow.record(latencyMillis);
                if (fixedPublishLatencyWindows != null) {
                    fixedPublishLatencyWindows.record(latencyMillis, System.currentTimeMillis());
                }
                synchronized (lastPublished) {
                    if (!hasPosition(metadata)) {
                        // A valid duplicate ACK may have no position. Retain its identity in normal logs.
                        log.warn("Acknowledged without position | key: {} | seq: {} | metadata: {} | last known: {}",
                                key, seq, metadata, lastPositioned.get(key));
                    }
                    if (seq < firstFailed.getOrDefault(key, Long.MAX_VALUE)) {
                        NavigableMap<Long, LongSeq> waiting = completed.computeIfAbsent(key, k -> new TreeMap<>());
                        waiting.put(seq, new LongSeq(seq, metadata));
                        LongSeq next;
                        while ((next = waiting.get(lastPublished.get(key).seq() + 1)) != null) {
                            processingSeq = next.seq();
                            validatePosition(key, next);
                            lastPublished.put(key, next);
                            waiting.remove(next.seq());
                        }
                        if (waiting.isEmpty()) {
                            completed.remove(key);
                        }
                    }
                }
            } catch (RuntimeException processingError) {
                markFailed(key, processingSeq);
                recordError("Producer acknowledgement processing failed", System.currentTimeMillis());
                log.error("Producer acknowledgement processing failed | key: {} | seq: {} | metadata: {}",
                        key, processingSeq, metadata, processingError);
                throw processingError;
            }
        });
    }

    private void validatePosition(String key, LongSeq message) {
        if (!hasPosition(message.metadata())) {
            return;
        }
        LongSeq previous = lastPositioned.get(key);
        if (previous != null && !message.metadata().isAfter(previous.metadata())) {
            log.error("Offset out of order writes | key: {} | new message: {} | previous message: {}",
                    key, message, previous);
            recordError("Offset out of order writes", System.currentTimeMillis());
        }
        lastPositioned.put(key, message);
    }

    private void recordSendFailure(String key, long seq, Throwable error) {
        sendFailures.incrementAndGet();
        markFailed(key, seq);
        recordError(error.getMessage() != null ? error.getMessage() : error.getClass().getName(),
                System.currentTimeMillis());
        log.error("Producer send failed | key: {} | seq: {}", key, seq, error);
    }

    private void markFailed(String key, long seq) {
        synchronized (lastPublished) {
            long barrier = firstFailed.merge(key, seq, Math::min);
            NavigableMap<Long, LongSeq> waiting = completed.get(key);
            if (waiting != null) {
                // Later successes can never close a terminal gap; do not retain them indefinitely.
                waiting.tailMap(barrier, true).clear();
                if (waiting.isEmpty()) {
                    completed.remove(key);
                }
            }
        }
    }

    private static boolean hasPosition(MessageMetadata metadata) {
        if (metadata.ledgerId() != null && metadata.entryId() != null) {
            return metadata.ledgerId() >= 0 && metadata.entryId() >= 0;
        }
        return metadata.offset() != null && metadata.offset() >= 0;
    }

    public long getSendAttempts() {
        return sendAttempts.get();
    }

    public long getSendFailures() {
        return sendFailures.get();
    }

    public long getAcknowledged() {
        return acknowledged.get();
    }

    public long getAcknowledgedBytes() {
        return acknowledgedBytes.get();
    }

    public LatencyMetricSnapshot getPublishLatencySnapshot() {
        return publishLatency.snapshot();
    }

    /**
     * Returns the windowed publish latency snapshot and resets the window recorder.
     * Each call only reflects samples recorded since the previous call.
     */
    public LatencyMetricSnapshot getPublishLatencyWindowSnapshot() {
        return publishLatencyWindow.snapshotAndReset();
    }

    public Map<String, LongSeq> getLastPublished() {
        synchronized (lastPublished) {
            return Collections.unmodifiableMap(new HashMap<>(lastPublished));
        }
    }

    public Map<String, ErrorOccurrence> getErrorDetails() {
        Map<String, ErrorOccurrence> copy = new HashMap<>();
        synchronized (errorDetails) {
            errorDetails.forEach((key, value) -> copy.put(
                    key,
                    new ErrorOccurrence(value.getCount(), value.getFirstSeenAtMillis(), value.getLastSeenAtMillis())));
        }
        return Collections.unmodifiableMap(copy);
    }

    private void recordError(String message, long timestampMillis) {
        synchronized (errorDetails) {
            errorDetails.compute(message, (k, existing) -> {
                if (existing == null) {
                    return ErrorOccurrence.firstSeen(timestampMillis);
                }
                existing.recordOccurrence(timestampMillis);
                return existing;
            });
        }
    }

    private int estimateMessageBytes(String key) {
        return (key == null ? 0 : key.getBytes(StandardCharsets.UTF_8).length) + messageSize;
    }

    /**
     * Closes the underlying producer and releases associated resources.
     *
     * @throws Exception if an error occurs while closing the producer
     */
    @Override
    public void close() throws Exception {
        producer.close();
    }
}
