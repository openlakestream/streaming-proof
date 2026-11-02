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

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.ProofProducer;
import io.lakestream.streaming.proof.common.records.ErrorOccurrence;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages a group of producer tasks for streaming proof tests. This class coordinates
 * multiple producers to generate messages at a controlled rate while maintaining
 * sequence tracking for verification.
 *
 * <p>Key features:
 * <ul>
 *   <li>Rate-limited message production using {@link UniformRateLimiter}</li>
 *   <li>Fair distribution of keys across multiple producers</li>
 *   <li>Sequence tracking per message key</li>
 *   <li>Aggregated producer statistics collection</li>
 * </ul>
 *
 * <p>For exactly-once processing, the class creates {@link KafkaExactlyOnceProofProducer}
 * instances that embed transactional processors for atomic read-process-write operations.
 *
 * <p>The class uses a virtual thread to coordinate message production across
 * all producer tasks, ensuring controlled message rates and fair task scheduling
 * with minimal resource overhead.
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewProducers config = new NewProducers(
 *     "test-producers",  // unique group ID
 *     "test-topic",     // topic to produce to
 *     4,                // number of producers
 *     100,              // number of unique keys
 *     5000,             // messages per second
 *     kafkaDriver       // messaging system driver
 * );
 *
 * ProofProducers producers = new ProofProducers(config);
 * producers.start();
 *
 * // Later, check production progress
 * CheckPoint checkpoint = producers.checkPoint();
 *
 * // Finally, stop all producers
 * producers.stop();
 * }</pre>
 *
 * @see ProofProducerTask
 * @see UniformRateLimiter
 * @see ProofDriver
 */
@Slf4j
public class ProofProducers {

    /** Configuration for creating new producers */
    private final NewProducers newProducers;
    private final FixedWindowLatencyRecorder fixedPublishLatencyWindows;

    /** The messaging system driver instance */
    private final ProofDriver driver;

    /** List of running producer tasks */
    private final List<ProofProducerTask> tasks;

    /** Virtual thread for the producer task */
    private Thread producerThread;

    /** Counter for round-robin task selection */
    private final AtomicLong index = new AtomicLong(0);

    /** Flag controlling the producer group's lifecycle */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /** Rate limiter for controlling message production speed */
    private final UniformRateLimiter rateLimiter;


    /**
     * Creates a new ProofProducers instance with the specified configuration.
     *
     * @param newProducers Configuration specifying the number of producers,
     *                     message rate, and driver settings
     */
    public ProofProducers(NewProducers newProducers, ProofDriver driver) {
        this.newProducers = newProducers;
        this.fixedPublishLatencyWindows = newProducers.publishLatencyWindowSeconds() > 0
                ? new FixedWindowLatencyRecorder(newProducers.publishLatencyWindowSeconds()) : null;
        this.driver = driver;
        this.tasks = new ArrayList<>(newProducers.producers());
        this.rateLimiter = new UniformRateLimiter(newProducers.msgRate());
    }

    /**
     * Initializes the driver and creates producer tasks with evenly distributed keys.
     * This method is called during start-up to prepare the producer group for operation.
     *
     * @throws IllegalArgumentException if the specified driver type is not supported
     */
    private void init() {
        // Calculate keys per producer
        int totalKeys = newProducers.keys();
        int producerCount = newProducers.producers();
        if (producerCount <= 0) {
            throw new IllegalArgumentException("Producer groups require at least one producer");
        }
        if (totalKeys <= 0) {
            throw new IllegalArgumentException("Producer groups require at least one key");
        }
        int activeProducerCount = Math.min(totalKeys, producerCount);
        if (activeProducerCount < producerCount) {
            log.warn("Requested {} producers but only {} keys available, reducing to {}",
                    producerCount, totalKeys, activeProducerCount);
        }
        int baseKeysPerProducer = totalKeys / activeProducerCount;
        int remainder = totalKeys % activeProducerCount;

        // Create tasks with their respective key counts
        for (int i = 0; i < activeProducerCount; i++) {
            ProofProducer producer;
            
            if (newProducers.transactional() && "kafka".equals(newProducers.driver().driverType())) {
                // Create exactly-once producer with embedded transactional processor
                // Cast to KafkaProofDriver to access exactly-once method
                if (driver instanceof io.lakestream.streaming.proof.driver.kafka.KafkaProofDriver) {
                    producer = ((io.lakestream.streaming.proof.driver.kafka.KafkaProofDriver) driver)
                            .createProducer(newProducers.topic(), newProducers.driver().driverConfigs(),
                                    newProducers.messageSize(), true);
                } else {
                    throw new IllegalStateException("Exactly-once semantics require Kafka driver");
                }
            } else {
                // Create normal producer
                producer = driver.createProducer(newProducers.topic(), newProducers.driver().driverConfigs(),
                        newProducers.messageSize());
            }
            
            int keyCount = baseKeysPerProducer + (i == activeProducerCount - 1 ? remainder : 0);
            tasks.add(new ProofProducerTask(producer, keyCount, newProducers.messageSize(),
                    fixedPublishLatencyWindows));
        }
    }


    /**
     * Collects and aggregates checkpoints from all producer tasks.
     * The checkpoint includes:
     * <ul>
     *   <li>Latest sequence number per key</li>
     *   <li>Count of failed sends</li>
     *   <li>List of failed sequence numbers</li>
     * </ul>
     *
     * @return A checkpoint containing aggregated producer statistics
     */
    public ProducerCheckpoint checkPoint() {
        ProducerCheckpoint checkpoint = new ProducerCheckpoint();
        for (ProofProducerTask task : tasks) {
            task.getLastPublished().forEach(checkpoint::addPublished);
            task.getErrorDetails().forEach(checkpoint::addErrorDetails);
        }
        return checkpoint;
    }

    public LatencyMetricSnapshot publishLatencyWindowSnapshot(long windowId) {
        return fixedPublishLatencyWindows == null ? null : fixedPublishLatencyWindows.snapshot(windowId);
    }

    public ProofWorkerMetricsSnapshot metricsSnapshot() {
        long sendAttempts = 0L;
        long acknowledgedMessages = 0L;
        long publishErrors = 0L;
        LatencyRecorder publishLatency = new LatencyRecorder();
        for (ProofProducerTask task : tasks) {
            sendAttempts += task.getSendAttempts();
            acknowledgedMessages += task.getAcknowledged();
            publishErrors += task.getErrorDetails().values().stream()
                    .mapToLong(ErrorOccurrence::getCount)
                    .sum();
            LatencyMetricSnapshot latencySnapshot = task.getPublishLatencySnapshot();
            publishLatency.mergeFrom(latencySnapshot);
        }
        return new ProofWorkerMetricsSnapshot(
                sendAttempts,
                acknowledgedMessages,
                tasks.stream().mapToLong(ProofProducerTask::getAcknowledgedBytes).sum(),
                publishErrors,
                0L,
                0L,
                publishLatency.snapshot(),
                null);
    }

    /**
     * Returns a windowed metrics snapshot using reset-on-read latency samples.
     * Counters (sendAttempts, acknowledged, errors) remain cumulative; only
     * the latency samples cover the window since the last call.
     */
    public ProofWorkerMetricsSnapshot windowedMetricsSnapshot() {
        long sendAttempts = 0L;
        long acknowledgedMessages = 0L;
        long publishErrors = 0L;
        LatencyRecorder publishLatency = new LatencyRecorder();
        for (ProofProducerTask task : tasks) {
            sendAttempts += task.getSendAttempts();
            acknowledgedMessages += task.getAcknowledged();
            publishErrors += task.getErrorDetails().values().stream()
                    .mapToLong(ErrorOccurrence::getCount)
                    .sum();
            LatencyMetricSnapshot latencySnapshot = task.getPublishLatencyWindowSnapshot();
            publishLatency.mergeFrom(latencySnapshot);
        }
        return new ProofWorkerMetricsSnapshot(
                sendAttempts,
                acknowledgedMessages,
                tasks.stream().mapToLong(ProofProducerTask::getAcknowledgedBytes).sum(),
                publishErrors,
                0L,
                0L,
                publishLatency.snapshot(),
                null);
    }

    /**
     * Stops all producer tasks and releases associated resources.
     * This method ensures graceful shutdown of the virtual thread and all producer tasks.
     *
     * @throws RuntimeException if an error occurs while closing producers
     */
    public void stop() {
        log.info("Stopping producers for topic {}", newProducers.topic());
        running.set(false);
        if (producerThread != null) {
            producerThread.interrupt();
        }
        
        
        Exception firstFailure = null;
        for (ProofProducerTask task : tasks) {
            try {
                task.close();
            } catch (Exception e) {
                log.error("Failed to close producer task for topic {}", newProducers.topic(), e);
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        log.info("Stopped producers for topic {}", newProducers.topic());
        if (firstFailure != null) {
            throw new RuntimeException(firstFailure);
        }
    }

    /**
     * Starts the producer group and begins message production.
     * Messages are produced at the configured rate using the rate limiter,
     * with tasks selected in round-robin fashion.
     *
     * Uses a virtual thread for lightweight concurrency.
     */
    public void start() {
        init();
        Runnable producerTask = () -> {
            while (running.get()) {
                final long intendedSendTime = rateLimiter.acquire();
                UniformRateLimiter.uninterruptibleSleepNs(intendedSendTime);
                tasks.get((int) (index.getAndIncrement() % tasks.size())).sendAsync();
            }
        };

        // Start the producer task on a virtual thread
        producerThread = Thread.ofVirtual()
                .name("proof-producer-" + newProducers.id())
                .start(producerTask);
    }
}
