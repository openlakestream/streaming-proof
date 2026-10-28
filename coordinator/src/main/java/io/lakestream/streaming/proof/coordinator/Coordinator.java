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
package io.lakestream.streaming.proof.coordinator;

import io.lakestream.streaming.proof.common.ProofValue;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.Checkpoints;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofDetails;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import io.lakestream.streaming.proof.worker.DriverCache;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * The Coordinator orchestrates distributed verification of messaging system guarantees.
 * 
 * <p>As the central control component, the Coordinator:
 * <ul>
 *   <li>Manages system-wide configurations for workers and messaging drivers</li>
 *   <li>Creates and manages proof verification tasks</li>
 *   <li>Distributes workloads across worker nodes</li>
 *   <li>Aggregates checkpoints to verify system-wide guarantees</li>
 *   <li>Detects violations of messaging system guarantees</li>
 *   <li>Reports verification results with detailed diagnostics</li>
 * </ul>
 *
 * <p>The Coordinator implements a distributed verification architecture where:
 * <ul>
 *   <li>Multiple workers can be deployed across different zones/regions</li>
 *   <li>Each worker can run multiple producers and consumers</li>
 *   <li>Checkpoints are aggregated to detect cross-worker issues</li>
 *   <li>Different messaging systems can be verified using the same framework</li>
 * </ul>
 *
 *
 * @see ProofTask
 * @see Configs
 * @see Proof
 * @see io.lakestream.streaming.proof.worker.Worker
 */
@Slf4j
@Getter
public class Coordinator {

    private static final Comparator<Proof> PROOF_START_TIME_DESC_COMPARATOR = Comparator
            .comparing(Coordinator::parseStartTime, Comparator.nullsLast(Comparator.reverseOrder()))
            .thenComparing(Proof::getId, Comparator.nullsLast(Comparator.naturalOrder()));

    /** System-wide configuration for workers and drivers */
    private Configs configs;
    
    /** Active proof tasks mapped by their unique identifiers */
    private final Map<String, ProofTask> proofs = new ConcurrentHashMap<>();

    private final DriverCache driverCache = new DriverCache();

    /**
     * Updates the system configuration with new worker and driver settings.
     * If no configuration exists, initializes a new one. Otherwise, merges
     * the new configuration with the existing one.
     *
     * @param configs The new configuration to apply
     */
    public void updateConfigs(Configs configs) {
        if (this.configs == null) {
            this.configs = configs;
        } else {
            this.configs.workers().putAll(configs.workers());
            this.configs.drivers().putAll(configs.drivers());
            if (configs.report() != null) {
                if (this.configs.report() == null) {
                    this.configs = new Configs(this.configs.workers(), this.configs.drivers(),
                            new java.util.HashMap<>(configs.report()));
                } else {
                    this.configs.report().putAll(configs.report());
                }
            }
        }
    }

    /**
     * Removes specified worker and driver configurations from the system.
     *
     * @param configs Configuration containing workers and drivers to remove
     */
    public void deleteConfigs(Configs configs) {
        configs.workers().forEach((k, v) -> this.configs.workers().remove(k));
        configs.drivers().forEach((k, v) -> this.configs.drivers().remove(k));
        if (configs.report() != null && this.configs.report() != null) {
            configs.report().forEach((k, v) -> this.configs.report().remove(k));
        }
    }

    /**
     * Removes all worker and driver configurations from the system.
     */
    public void clearAllConfigs() {
        if (this.configs != null) {
            this.configs.workers().clear();
            this.configs.drivers().clear();
            if (this.configs.report() != null) {
                this.configs.report().clear();
            }
            log.info("All configurations have been cleared");
        }
    }

    /**
     * Creates and starts a new proof test with the specified configuration.
     * The method validates the driver and features before creating the test,
     * assigns a unique identifier, and initializes a new {@link ProofTask}.
     *
     * @param proof The proof test configuration
     * @throws IllegalArgumentException if the specified driver is not found or
     *         if any requested features are not supported
     */
    public void createProof(Proof proof) {
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().admin();
        if (!configs.drivers().containsKey(driverName)) {
            throw new IllegalArgumentException("Driver " + driverName + " not found. "
                    + "Available drivers: " + configs.drivers().keySet());
        }
        if (!Util.SUPPORTED_PROOF_FEATS.containsAll(proof.getFeatures())) {
            throw new IllegalArgumentException("Unsupported proof features: " + proof.getFeatures()
                    + ". Supported features: " + Util.SUPPORTED_PROOF_FEATS);
        }
        if (proof.getConsumeDelay() > 0 && proof.getConsumeDelay() >= proof.getTimeout()) {
            throw new IllegalArgumentException("Consume delay must be less than timeout.");
        }
        if (proof.getFinalWaitSeconds() != null && proof.getFinalWaitSeconds() < 0) {
            throw new IllegalArgumentException("Final wait seconds must be 0 or greater.");
        }
        if (proof.getProducers() < 1) {
            throw new IllegalArgumentException("Producers must be at least 1.");
        }
        if (proof.getKeys() < 1) {
            throw new IllegalArgumentException("Keys must be at least 1.");
        }
        if (proof.getMsgRate() < 1) {
            throw new IllegalArgumentException("Message rate must be at least 1.");
        }
        if (proof.getMessageSize() < ProofValue.MIN_SIZE) {
            throw new IllegalArgumentException("Message size must be at least " + ProofValue.MIN_SIZE
                    + " bytes to hold the sequence number, got " + proof.getMessageSize() + ".");
        }
        String consumerDriverName = proof.getDrivers() == null
                ? proof.getDriver() : proof.getDrivers().consumer();
        if (!configs.drivers().containsKey(consumerDriverName)) {
            throw new IllegalArgumentException("Consumer driver " + consumerDriverName + " not found.");
        }
        if (configs.drivers().get(consumerDriverName).driverConfigs()
                .containsKey("pulsar.checkpoint.restart.messages")) {
            throw new IllegalArgumentException(
                    "Use per-proof pulsar.checkpointRestartMessages, not the driver-wide restart setting.");
        }
        validateScalableTopicSettings(proof);
        if (proof.getPulsar() != null && proof.getPulsar().isCheckpointRecoveryEnabled()
                && (!"pulsar".equals(configs.drivers().get(driverName).driverType())
                    || !"pulsar".equals(configs.drivers().get(consumerDriverName).driverType()))) {
            throw new IllegalArgumentException("Checkpoint recovery requires Pulsar admin and consumer drivers.");
        }
        String proofID = RandomStringUtils.secure().nextAlphanumeric(5);
        proof.setId(proofID);
        ProofTask task = new ProofTask(proof, configs, driverCache.getDriver(driverName,
                configs.drivers().get(driverName)));
        proofs.put(proof.getId(), task);
        task.start();
    }

    /** Scalable topics take initial segments and a consumer type instead of partitions and a subscription type. */
    private static void validateScalableTopicSettings(Proof proof) {
        PulsarProofConfig pulsar = proof.getPulsar();
        if (pulsar != null && pulsar.getCheckpointRestartMessages() != null) {
            if (pulsar.getCheckpointRestartMessages() <= 0) {
                throw new IllegalArgumentException("pulsar.checkpointRestartMessages must be positive when set.");
            }
            if (!proof.isScalableTopic() || !"checkpoint".equals(pulsar.getConsumerType())
                    || proof.getConsumers() != 1) {
                throw new IllegalArgumentException(
                        "Checkpoint recovery requires a scalable topic, consumerType=checkpoint and consumers=1.");
            }
            String topic = proof.getTopic();
            if (topic != null && topic.contains("://") && !topic.startsWith("topic://")) {
                throw new IllegalArgumentException(
                        "Checkpoint recovery requires a topic:// URI or an unqualified topic name.");
            }
        }
        if (!proof.isScalableTopic()) {
            if (pulsar != null && (pulsar.getInitialSegments() != null || pulsar.getConsumerType() != null)) {
                throw new IllegalArgumentException(
                        "pulsar.initialSegments and pulsar.consumerType require pulsar.scalableTopicEnabled.");
            }
            return;
        }
        if (proof.getPartitions() != null) {
            throw new IllegalArgumentException(
                    "Scalable topics have no partitions, use pulsar.initialSegments instead.");
        }
        if (pulsar.getConsumerConfig() != null) {
            throw new IllegalArgumentException(
                    "Scalable topics have no subscription types, use pulsar.consumerType (stream, queue or "
                            + "checkpoint) instead of pulsar.consumerConfig.");
        }
        if (pulsar.getConsumerType() != null
                && !List.of("stream", "queue", "checkpoint").contains(pulsar.getConsumerType())) {
            throw new IllegalArgumentException("pulsar.consumerType must be stream, queue or checkpoint.");
        }
    }

    /**
     * Stops a running proof test without removing it from the system.
     * This method halts the test execution but preserves the test configuration
     * and results for later reference.
     *
     * @param proofID The unique identifier of the proof test to stop
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public void stopProof(String proofID) {
        ProofTask task = proofs.get(proofID);
        task.requestStop();
    }

    /**
     * Completely removes a proof test from the system.
     * This method stops the test if it's running, cleans up all associated resources
     * including the messaging topic, and removes the test from the coordinator's registry.
     *
     * @param proofID The unique identifier of the proof test to remove
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public void removeProof(String proofID) {
        ProofTask task = proofs.get(proofID);
        task.stop();
        task.remove();
        proofs.remove(proofID);
    }

    /**
     * Retrieves a list of all proof tests in the system.
     * The returned list is an immutable copy of the current proof tests,
     * ensuring thread safety and preventing external modifications.
     *
     * @return An immutable list of all {@link Proof} configurations currently
     *         registered in the system
     */
    public List<Proof> listProofs() {
        return List.copyOf(proofs.values().stream()
                .map(ProofTask::getProof)
                .sorted(PROOF_START_TIME_DESC_COMPARATOR)
                .toList());
    }

    public List<java.util.Map<String, Object>> listProofsWithStatus() {
        return proofs.values().stream()
                .sorted(Comparator.comparing(
                        ProofTask::getProof,
                        PROOF_START_TIME_DESC_COMPARATOR))
                .map(task -> {
                    java.util.Map<String, Object> map = Util.JSON_MAPPER.convertValue(
                            task.getProof(),
                            new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() { });
                    ProofReport report = task.getReport();
                    map.put("status", report.status());
                    return map;
                })
                .toList();
    }

    private static LocalDateTime parseStartTime(Proof proof) {
        String startTime = proof.getStartTime();
        if (startTime == null || startTime.isBlank()) {
            return null;
        }
        try {
            return LocalDateTime.parse(startTime);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Retrieves detailed information about a specific proof test.
     * This method provides comprehensive information including:
     * <ul>
     *   <li>The test configuration</li>
     *   <li>Current execution status</li>
     *   <li>Checkpoints for both producers and consumers</li>
     *   <li>Latest verification results</li>
     *   <li>Any failed verification attempts</li>
     * </ul>
     *
     * @param proofID The unique identifier of the proof test to retrieve
     * @return A {@link ProofDetails} object containing the test configuration,
     *         summary, and checkpoint information
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public ProofDetails getProofDetails(String proofID) {
        ProofTask task = proofs.get(proofID);
        if (task == null) {
            return null;
        }
        return task.getDetails();
    }

    public ProofDetails getProofSummary(String proofID) {
        ProofTask task = proofs.get(proofID);
        if (task == null) {
            return null;
        }
        Checkpoints checkpoints = new Checkpoints(
                task.getInCheck(),
                task.getLatestProducerCheckpoint(),
                task.getLatestConsumerCheckpoint(),
                task.getLastVerifiedProducerCheckpoint(),
                task.getLastVerifiedConsumerCheckpoint(),
                task.getLastFailedProducerCheckpoint(),
                task.getLastFailedConsumerCheckpoint());
        return new ProofDetails(task.getProof(), task.getSummary(), checkpoints, null, null, null, null);
    }

    public ProofReport getProofReport(String proofID) {
        ProofTask task = proofs.get(proofID);
        if (task == null) {
            return null;
        }
        return task.getReport();
    }

    /**
     * Gracefully shuts down the coordinator by stopping all running proof tests.
     * This method ensures that all tests are properly stopped and resources are
     * cleaned up before the coordinator is terminated.
     */
    public void close() {
        proofs.values().forEach(ProofTask::stop);
        driverCache.close();
    }
}
