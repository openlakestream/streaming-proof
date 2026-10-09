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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.lakestream.streaming.proof.common.ProofValue;
import io.lakestream.streaming.proof.common.WebhookConfig;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a streaming proof test configuration that validates messaging system guarantees.
 * This class uses the Builder pattern to create test configurations with customizable parameters
 * for validating different messaging system features and performance characteristics.
 *
 * <p>A proof test can validate various messaging guarantees such as:
 * <ul>
 *   <li>Exactly-once delivery</li>
 *   <li>At-least-once delivery</li>
 *   <li>Message ordering</li>
 *   <li>Performance under different loads</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Proof proof = Proof.builder()
 *     .name("kafka-ordering-test")
 *     .driver("kafka")
 *     .features(List.of("ordering", "at_least_once"))
 *     .topic("test-topic")
 *     .partitions(8)
 *     .msgRate(5000)
 *     .kafka(KafkaProofConfig.builder()
 *         .topicConfig(java.util.Map.of("unclean.leader.election.enable", "true"))
 *         .build())
 *     .build();
 * }</pre>
 *
 * @see ProofDetails
 * @see Driver
 */
@Data
@Builder
@lombok.AllArgsConstructor
@lombok.NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Proof {

    /** Partitions of the topic, or initial segments of a Pulsar scalable topic, when not set */
    public static final int DEFAULT_PARTITIONS = 10;

    /** Unique identifier for the proof test */
    private String id;

    /** Human-readable name for the proof test */
    private String name;

    /**
     * The messaging system driver to use for this proof.
     * The driver configuration is stored separately in {@link Configs}.
     */
    private String driver;

    /**
     * The driver configuration for the proof.
     * This includes driver name for the admin, producer, and consumer.
     */
    private Drivers drivers;

    /**
     * List of messaging guarantees to validate.
     * Supported features include: exactly-once, at-least-once, ordering
     */
    private List<String> features;

    /** Detailed description of the proof test's purpose and expectations */
    private String description;

    /** The topic name to use for message production and consumption */
    private String topic;

    /**
     * Number of partitions for the topic, {@value #DEFAULT_PARTITIONS} when not set.
     * Higher partition count enables greater parallelism but may affect ordering guarantees.
     * Not allowed for Pulsar scalable topics, which use {@link PulsarProofConfig#getInitialSegments()}.
     */
    private Integer partitions;

    /**
     * Number of producer instances to create.
     * Multiple producers can increase throughput and test concurrent message production.
     */
    @Builder.Default
    private int producers = 4;

    /**
     * Number of consumer instances to create.
     * Multiple consumers enable parallel message processing and consumption pattern testing.
     */
    @Builder.Default
    private int consumers = 4;

    /**
     * Target message production rate in messages per second.
     * Used to control the load on the messaging system.
     */
    @Builder.Default
    private int msgRate = 1000;

    /**
     * Number of unique message keys to use.
     * Affects message distribution across partitions and ordering guarantees.
     */
    @Builder.Default
    private int keys = 100;

    /**
     * Total size in bytes of each message value, including the 8-byte sequence number.
     * Sizes above the minimum pad each message so that storage-path behaviour
     * (offload triggers, tiered-storage reads, bandwidth) can be exercised at a
     * realistic scale. The padding is filler and is never verified.
     */
    @Builder.Default
    private int messageSize = ProofValue.MIN_SIZE;

    /**
     * Interval between checkpoints in seconds.
     * Checkpoints track message delivery progress and verify guarantees.
     */
    @Builder.Default
    private int checkPointInterval = 60;

    /**
     * Maximum time in seconds to wait for message verification.
     * Test fails if verification exceeds this timeout.
     */
    @Builder.Default
    private int timeout = 180;

    /**
     * Delay in seconds to simulate catchup reading.
     * The value must be 0 or greater than timeout.
     * Default is 0, means no delay.
     */
    @Builder.Default
    private int consumeDelay = 0;

    /**
     * Total duration of the proof test in seconds.
     * Default is 8 hours (28800 seconds).
     */
    @Builder.Default
    private int duration = 28800;

    /**
     * Additional seconds to wait during final verification after producers stop.
     * Null uses coordinator/global defaults. A value of 0 means no retry wait.
     */
    private Integer finalWaitSeconds;

    /** Human-readable timestamp when the proof test started */
    private String startTime;

    /**
     * Optional report sampling interval in seconds.
     * A value of 0 lets the coordinator derive a suitable interval automatically.
     */
    @Builder.Default
    private int timeSeriesInterval = 0;

    /**
     * Maximum acceptable peak verification stall in seconds.
     * If the run's peak stall exceeds this value the result is marked failed.
     * Default is 0, which disables the stall limit.
     */
    @Builder.Default
    private int maxStallSeconds = 0;

    /** Webhook configuration for notifications */
    private WebhookConfig webhookConfig;

    /** Per-proof Pulsar-specific configuration */
    private PulsarProofConfig pulsar;

    /** Per-proof Kafka-specific configuration */
    private KafkaProofConfig kafka;

    /** Whether the proof runs on a Pulsar 5 scalable topic instead of a classic topic */
    @JsonIgnore
    public boolean isScalableTopic() {
        return pulsar != null && pulsar.isScalableTopicEnabled();
    }
}
