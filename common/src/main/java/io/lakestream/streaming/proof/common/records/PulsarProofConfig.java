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
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-proof Pulsar-specific configuration.
 *
 * <p>Classic topics take an optional consumer configuration that is passed through to
 * Pulsar's {@code ConsumerBuilder.loadConf()} at consumer creation time. Pulsar 5 scalable
 * topics have no partitions or subscription types and take initial segments and a consumer
 * type instead.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PulsarProofConfig {

    /**
     * Consumer configuration map passed to {@code ConsumerBuilder.loadConf()} for classic topics.
     * Keys must match Pulsar's ConsumerConfigurationData field names (case-sensitive).
     * Example keys: "subscriptionType" ("Exclusive", "Failover", "Shared", "Key_Shared"),
     * "receiverQueueSize", "ackTimeoutMillis".
     */
    private Map<String, Object> consumerConfig;

    /** Whether the proof runs on a Pulsar 5 scalable topic ({@code topic://}) instead of a classic topic. */
    private boolean scalableTopicEnabled;

    /**
     * Number of initial segments of the scalable topic, {@value Proof#DEFAULT_PARTITIONS} when not set.
     * Replaces the proof's partitions, which scalable topics do not have.
     */
    private Integer initialSegments;

    /**
     * Consumer type for Pulsar 5 scalable topics: "stream" (the default) consumes in per-key
     * order, "queue" consumes without ordering and is verified like a Shared subscription, and
     * "checkpoint" reads in per-key order with no broker-managed position.
     */
    private String consumerType;

    /**
     * Returns whether consumers receive messages without per-key ordering: a Shared subscription,
     * or queue consumers on a scalable topic.
     */
    @JsonIgnore
    public boolean isSharedMode() {
        return isSharedMode(consumerConfig, consumerType);
    }

    /** Same as {@link #isSharedMode()}, for callers that only have the two settings. */
    public static boolean isSharedMode(Map<String, Object> consumerConfig, String consumerType) {
        return "queue".equals(consumerType)
                || consumerConfig != null && "Shared".equals(consumerConfig.get("subscriptionType"));
    }
}
