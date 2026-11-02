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

import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages producer and consumer instances for streaming proof verification on a worker node.
 * 
 * <p>The Worker is a distributed component that:
 * <ul>
 *   <li>Manages multiple concurrent producer and consumer groups</li>
 *   <li>Creates, configures, and monitors messaging clients</li>
 *   <li>Collects detailed checkpoints for verification analysis</li>
 *   <li>Provides thread-safe operation in multi-tenant environments</li>
 *   <li>Optimizes resource usage through driver instance caching</li>
 *   <li>Supports horizontal scaling across multiple worker nodes</li>
 * </ul>
 *
 * <p>Workers operate as part of a distributed verification system where:
 * <ul>
 *   <li>Each worker can be deployed in a different zone/region</li>
 *   <li>Multiple verification tasks can run concurrently on each worker</li>
 *   <li>Producers generate sequentially numbered messages with unique keys</li>
 *   <li>Consumers track received sequences using range-based checkpoints</li>
 *   <li>The coordinator periodically collects these checkpoints for analysis</li>
 * </ul>
 *
 * <p>Each producer and consumer group is identified by a unique ID, allowing
 * multiple verification scenarios to run concurrently on the same worker node.
 *
 * @see ProofProducers
 * @see ProofConsumers
 * @see io.lakestream.streaming.proof.common.records.ProducerCheckpoint
 * @see io.lakestream.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.lakestream.streaming.proof.coordinator.Coordinator
 */
public class Worker {

    /** Thread-safe map of producer groups indexed by their IDs */
    private final Map<String, ProofProducers> producers = new ConcurrentHashMap<>();

    /** Thread-safe map of consumer groups indexed by their IDs */
    private final Map<String, ProofConsumers> consumers = new ConcurrentHashMap<>();

    /** Cache for driver instances to avoid redundant initializations */
    private final DriverCache driverCache = new DriverCache();

    /**
     * Starts a new group of producers with the specified configuration.
     *
     * @param newProducers Configuration for the producer group to start
     */
    public void startProducers(NewProducers newProducers) {
        ProofProducers producer = new ProofProducers(newProducers,
                driverCache.getDriver(newProducers.driverName(), newProducers.driver()));
        producers.put(newProducers.id(), producer);
        producer.start();
    }

    /**
     * Stops a group of producers identified by the given ID.
     *
     * <p>Note: The producer is not removed from the registry immediately to allow
     * the final checkpoint retrieval during verification. The producer will be
     * removed from the registry when {@link #removeProducers(String)} is called.
     *
     * @param id The unique identifier of the producer group to stop
     */
    public void stopProducers(String id) {
        ProofProducers producer = producers.get(id);
        if (producer != null) {
            producer.stop();
        }
    }

    /**
     * Removes a stopped producer group from the registry.
     *
     * @param id The unique identifier of the producer group to remove
     */
    public void removeProducers(String id) {
        producers.remove(id);
    }

    /**
     * Retrieves the current checkpoint for a producer group.
     *
     * @param id The unique identifier of the producer group
     * @return A checkpoint containing producer statistics and sequence information
     */
    public ProducerCheckpoint producerCheckPoint(String id) {
        ProofProducers producer = producers.get(id);
        if (producer == null) {
            return new ProducerCheckpoint();
        }
        return producer.checkPoint();
    }

    /**
     * Starts a new group of consumers with the specified configuration.
     *
     * @param newConsumers Configuration for the consumer group to start
     */
    public void startConsumers(NewConsumers newConsumers) {
        ProofConsumers consumer = new ProofConsumers(newConsumers,
                driverCache.getDriver(newConsumers.driverName(), newConsumers.driver()));
        consumers.put(newConsumers.id(), consumer);
        consumer.start();
    }

    /**
     * Stops a group of consumers identified by the given ID and removes the
     * associated producer group from the registry.
     *
     * <p>This method should be called after the final verification is complete
     * to clean up both consumers and any producer that was kept for checkpoint
     * retrieval.
     *
     * @param id The unique identifier of the consumer group to stop and remove
     */
    public void stopAndRemoveConsumers(String id) {
        ProofConsumers consumer = consumers.get(id);
        if (consumer != null) {
            consumer.stop();
            consumers.remove(id);
        }
        // Clean up any stopped producer that was kept for final checkpoint retrieval
        removeProducers(id);
    }

    /**
     * Retrieves the current checkpoint for a consumer group.
     *
     * @param id The unique identifier of the consumer group
     * @return A checkpoint containing consumer statistics and sequence information
     */
    public ConsumerCheckPoint consumerCheckPoint(String id) {
        ProofConsumers consumer = consumers.get(id);
        if (consumer == null) {
            return new ConsumerCheckPoint();
        }
        return consumer.checkPoint();
    }

    /**
     * Applies high watermarks to a consumer group, allowing verified sequence
     * ranges to be trimmed and reducing memory usage for long-running proofs.
     *
     * @param id The unique identifier of the consumer group
     * @param watermarks A map of key to high watermark sequence number
     */
    public void applyConsumerWatermarks(String id, Map<String, Long> watermarks) {
        ProofConsumers consumer = consumers.get(id);
        if (consumer != null) {
            consumer.applyHighWatermarks(watermarks);
        }
    }

    /**
     * Performs cleanup by stopping all producer and consumer groups managed by this worker.
     * This method ensures proper shutdown by iterating through all registered producers
     * and consumers and stopping them.
     *
     * <p>This method should be called when shutting down the worker node to ensure
     * all resources are properly released and all message processing is stopped.
     */
    public void close() {
        producers.values().forEach(ProofProducers::stop);
        consumers.values().forEach(ProofConsumers::stop);
        driverCache.close();
    }

    /**
     * Retrieves detailed checkpoints for each consumer in a consumer group.
     * This provides more granular information than the aggregated checkpoint,
     * allowing analysis of consumption patterns per individual consumer.
     *
     * @param id The unique identifier of the consumer group
     * @return A map of consumer names to their individual checkpoints
     */
    public Map<String, ConsumerCheckPoint> consumerCheckPointDetails(String id) {
        ProofConsumers consumer = consumers.get(id);
        if (consumer == null) {
            return Map.of();
        }
        return consumer.checkPointDetails();
    }

    public LatencyMetricSnapshot publishLatencyWindowSnapshot(String id, long windowId) {
        ProofProducers producer = producers.get(id);
        return producer == null ? null : producer.publishLatencyWindowSnapshot(windowId);
    }

    public ProofWorkerMetricsSnapshot metricsSnapshot(String id) {
        ProofProducers producer = producers.get(id);
        ProofConsumers consumer = consumers.get(id);
        ProofWorkerMetricsSnapshot producerMetrics = producer == null ? null : producer.metricsSnapshot();
        ProofWorkerMetricsSnapshot consumerMetrics = consumer == null ? null : consumer.metricsSnapshot();
        return new ProofWorkerMetricsSnapshot(
                producerMetrics == null ? 0L : producerMetrics.sendAttempts(),
                producerMetrics == null ? 0L : producerMetrics.acknowledgedMessages(),
                producerMetrics == null ? 0L : producerMetrics.acknowledgedBytes(),
                producerMetrics == null ? 0L : producerMetrics.publishErrors(),
                consumerMetrics == null ? 0L : consumerMetrics.receivedMessages(),
                consumerMetrics == null ? 0L : consumerMetrics.receivedBytes(),
                producerMetrics == null ? null : producerMetrics.publishLatency(),
                consumerMetrics == null ? null : consumerMetrics.endToEndLatency());
    }

    /**
     * Returns a windowed metrics snapshot where latency samples only cover the
     * interval since the last windowed read.  Counters remain cumulative.
     */
    public ProofWorkerMetricsSnapshot windowedMetricsSnapshot(String id) {
        ProofProducers producer = producers.get(id);
        ProofConsumers consumer = consumers.get(id);
        ProofWorkerMetricsSnapshot producerMetrics =
                producer == null ? null : producer.windowedMetricsSnapshot();
        ProofWorkerMetricsSnapshot consumerMetrics =
                consumer == null ? null : consumer.windowedMetricsSnapshot();
        return new ProofWorkerMetricsSnapshot(
                producerMetrics == null ? 0L : producerMetrics.sendAttempts(),
                producerMetrics == null ? 0L : producerMetrics.acknowledgedMessages(),
                producerMetrics == null ? 0L : producerMetrics.acknowledgedBytes(),
                producerMetrics == null ? 0L : producerMetrics.publishErrors(),
                consumerMetrics == null ? 0L : consumerMetrics.receivedMessages(),
                consumerMetrics == null ? 0L : consumerMetrics.receivedBytes(),
                producerMetrics == null ? null : producerMetrics.publishLatency(),
                consumerMetrics == null ? null : consumerMetrics.endToEndLatency());
    }
}
