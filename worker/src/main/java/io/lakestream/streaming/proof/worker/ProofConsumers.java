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

import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages a group of consumer tasks for streaming proof tests. This class is responsible
 * for creating, starting, monitoring, and stopping multiple consumer instances that verify
 * message delivery guarantees.
 *
 * <p>The class provides functionality for:
 * <ul>
 *   <li>Creating and managing multiple consumer tasks</li>
 *   <li>Initializing consumers with the appropriate messaging system driver</li>
 *   <li>Collecting and aggregating consumer statistics</li>
 *   <li>Monitoring message delivery guarantees</li>
 * </ul>
 *
 * @see ProofConsumerTask
 * @see ProofDriver
 * @see ConsumerCheckPoint
 */
@Slf4j
public class ProofConsumers {

    /** Configuration for creating new consumers */
    private final NewConsumers newConsumers;
    
    /** The messaging system driver instance */
    private final ProofDriver driver;
    
    /** List of running consumer tasks */
    private final List<ProofConsumerTask> tasks;

    /**
     * Creates a new ProofConsumers instance with the specified configuration and driver.
     *
     * @param newConsumers Configuration specifying the number of consumers,
     *                    topic, partitions, and other settings
     * @param driver The messaging system driver instance to use for creating consumers
     */
    public ProofConsumers(NewConsumers newConsumers, ProofDriver driver) {
        this.newConsumers = newConsumers;
        this.driver = driver;
        tasks = new ArrayList<>(newConsumers.consumers());
    }

    /**
     * Initializes the driver and starts all consumer tasks.
     * Creates the specified number of consumers and assigns them to consume
     * from the configured topic.
     *
     * @throws IllegalArgumentException if the specified driver type is not supported
     */
    public void start() {
        Map<String, Object> configs = new HashMap<>(newConsumers.driver().driverConfigs());
        if (newConsumers.pulsarConsumerConfig() != null) {
            configs.put("pulsar.consumer.config", newConsumers.pulsarConsumerConfig());
        }
        if (newConsumers.pulsarConsumerType() != null) {
            configs.put("pulsar.consumer.type", newConsumers.pulsarConsumerType());
        }
        Integer restartMessages = newConsumers.pulsarCheckpointRestartMessages();
        configs.put("pulsar.checkpoint.restart.messages", restartMessages == null ? 0 : restartMessages);
        boolean recoveryMode = restartMessages != null && restartMessages > 0;
        boolean sharedMode = PulsarProofConfig.isSharedMode(
                newConsumers.pulsarConsumerConfig(), newConsumers.pulsarConsumerType());
        for (int i = 0; i < newConsumers.consumers(); i++) {
            ProofConsumerTask task = new ProofConsumerTask(sharedMode, newConsumers.messageSize(), recoveryMode);
            ProofConsumer proofConsumer = driver.createConsumer(newConsumers.topic(), newConsumers.partitions(),
                    newConsumers.consumeDelayMs(), configs, task);
            task.setConsumer(proofConsumer);
            tasks.add(task);
        }
    }

    /**
     * Collects and aggregates checkpoints from all consumer tasks.
     *
     * @return A checkpoint containing aggregated consumer statistics
     */
    public ConsumerCheckPoint checkPoint() {
        ConsumerCheckPoint checkpoint = new ConsumerCheckPoint();
        for (ProofConsumerTask task : tasks) {
            checkpoint.setCheckpointReplayCount(
                    checkpoint.getCheckpointReplayCount() + task.getCheckpointReplayCount());
            task.getTrimmedConsumed().forEach(checkpoint::addKey);
            task.getWriteDupsOrOutOrder().forEach(checkpoint::addWriteDupsOrOutOrder);
        }
        return checkpoint;
    }

    /**
     * Collects individual checkpoints from each consumer task.
     * This provides more detailed information than the aggregated checkpoint,
     * allowing analysis of consumption patterns per consumer.
     *
     * @return A map of consumer names to their individual checkpoints
     */
    public Map<String, ConsumerCheckPoint> checkPointDetails() {
        Map<String, ConsumerCheckPoint> result = new HashMap<>();
        for (ProofConsumerTask task : tasks) {
            ConsumerCheckPoint checkPoint = new ConsumerCheckPoint();
            checkPoint.setCheckpointReplayCount(task.getCheckpointReplayCount());
            task.getTrimmedConsumed().forEach(checkPoint::addKey);
            task.getWriteDupsOrOutOrder().forEach(checkPoint::addWriteDupsOrOutOrder);
            result.put(task.getConsumerName(), checkPoint);
        }
        return result;
    }

    public ProofWorkerMetricsSnapshot metricsSnapshot() {
        long receivedMessages = 0L;
        long receivedBytes = 0L;
        LatencyRecorder endToEndLatency = new LatencyRecorder();
        for (ProofConsumerTask task : tasks) {
            receivedMessages += task.getReceivedMessages().get();
            receivedBytes += task.getReceivedBytes().get();
            LatencyMetricSnapshot latencySnapshot = task.getEndToEndLatencySnapshot();
            endToEndLatency.mergeFrom(latencySnapshot);
        }
        return new ProofWorkerMetricsSnapshot(
                0L,
                0L,
                0L,
                0L,
                receivedMessages,
                receivedBytes,
                null,
                endToEndLatency.snapshot());
    }

    /**
     * Returns a windowed metrics snapshot using reset-on-read latency samples.
     * Counters (receivedMessages, receivedBytes) remain cumulative; only
     * the latency samples cover the window since the last call.
     */
    public ProofWorkerMetricsSnapshot windowedMetricsSnapshot() {
        long receivedMessages = 0L;
        long receivedBytes = 0L;
        LatencyRecorder endToEndLatency = new LatencyRecorder();
        for (ProofConsumerTask task : tasks) {
            receivedMessages += task.getReceivedMessages().get();
            receivedBytes += task.getReceivedBytes().get();
            LatencyMetricSnapshot latencySnapshot = task.getEndToEndLatencyWindowSnapshot();
            endToEndLatency.mergeFrom(latencySnapshot);
        }
        return new ProofWorkerMetricsSnapshot(
                0L,
                0L,
                0L,
                0L,
                receivedMessages,
                receivedBytes,
                null,
                endToEndLatency.snapshot());
    }

    /**
     * Applies high watermarks to all consumer tasks, allowing them to trim
     * verified sequence ranges and reduce memory usage.
     *
     * @param watermarks A map of key to high watermark sequence number
     */
    public void applyHighWatermarks(Map<String, Long> watermarks) {
        for (ProofConsumerTask task : tasks) {
            task.applyHighWatermarks(watermarks);
        }
    }

    /**
     * Stops all consumer tasks and releases associated resources.
     *
     * @throws RuntimeException if an error occurs while closing consumers
     */
    public void stop() {
        log.info("Stopping consumers for topic {}", newConsumers.topic());
        Exception firstFailure = null;
        for (ProofConsumerTask task : tasks) {
            try {
                task.close();
            } catch (Exception e) {
                log.error("Failed to close consumer task for topic {}", newConsumers.topic(), e);
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        log.info("Stopped consumers for topic {}", newConsumers.topic());
        if (firstFailure != null) {
            throw new RuntimeException(firstFailure);
        }
    }
}
