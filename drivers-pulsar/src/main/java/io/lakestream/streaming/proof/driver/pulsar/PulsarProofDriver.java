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
package io.lakestream.streaming.proof.driver.pulsar;

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.ProofProducer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.v5.auth.AuthenticationFactory;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Pulsar implementation of the ProofDriver interface that manages Pulsar resources
 * and creates producers and consumers for streaming proof verification.
 *
 * <p>This driver provides:
 * <ul>
 *   <li>Topic management with namespace and tenant handling</li>
 *   <li>Producer creation with at-least-once delivery guarantees</li>
 *   <li>Consumer creation with subscription and acknowledgment management</li>
 *   <li>Message ID tracking for reliable verification</li>
 *   <li>Support for Pulsar's authentication and authorization mechanisms</li>
 *   <li>Pulsar 5 scalable topics, addressed by their {@code topic://} name, using the V5 client</li>
 * </ul>
 *
 * <p>The driver leverages Pulsar's features to enable:
 * <ul>
 *   <li>Sequence-based verification of delivery guarantees</li>
 *   <li>Key-based message routing to partitions</li>
 *   <li>Message ID-based tracing for debugging delivery issues</li>
 *   <li>Subscription management for scalable consumption</li>
 * </ul>
 *
 * @see io.lakestream.streaming.proof.common.ProofDriver
 * @see io.lakestream.streaming.proof.driver.pulsar.PulsarAtLeastOnceProofProducer
 * @see io.lakestream.streaming.proof.driver.pulsar.PulsarAtLeastOnceProofConsumer
 */
@Slf4j
public class PulsarProofDriver implements ProofDriver {

    /** Configuration key for Pulsar service URL */
    private static final String PULSAR_SERVICE_URL = "pulsar.service.url";

    /** Configuration key for Pulsar admin URL */
    private static final String PULSAR_ADMIN_URL = "pulsar.admin.url";

    /** Configuration key for authentication token */
    private static final String PULSAR_AUTH_TOKEN = "pulsar.auth.token";

    /** Per-proof consumer type of a scalable topic, "stream" (default), "queue" or "checkpoint" */
    private static final String PULSAR_CONSUMER_TYPE = "pulsar.consumer.type";

    /** Default namespace for topics */
    private static final String DEFAULT_NAMESPACE = "public/default";

    /** Default tenant for topics */
    private static final String DEFAULT_TENANT = "public";

    /** Pulsar client for producer/consumer operations on classic topics */
    private PulsarClient client;

    /** Pulsar V5 client for producer/consumer operations on scalable topics, created on first use */
    private org.apache.pulsar.client.api.v5.PulsarClient scalableClient;

    /** Connection settings, kept to create the V5 client on first use */
    private String serviceUrl;
    private String authToken;

    /** Pulsar admin client for topic management */
    private PulsarAdmin admin;

    /**
     * Initializes the driver with Pulsar-specific configurations.
     *
     * @param configs Configuration map containing Pulsar settings
     */
    @Override
    public void init(Map<String, Object> configs) {
        try {
            this.serviceUrl = (String) configs.getOrDefault(PULSAR_SERVICE_URL, "pulsar://pulsar:6650");
            String adminUrl = (String) configs.getOrDefault(PULSAR_ADMIN_URL, "http://pulsar:8080");
            this.authToken = (String) configs.get(PULSAR_AUTH_TOKEN);

            ClientBuilder clientBuilder = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .operationTimeout(30, TimeUnit.SECONDS);

            // Add token authentication if configured
            if (authToken != null && !authToken.isEmpty()) {
                clientBuilder.authentication(
                    "org.apache.pulsar.client.impl.auth.AuthenticationToken",
                    authToken
                );
            }

            // Apply any additional client configurations
            configs.forEach((key, value) -> {
                if (key.startsWith("pulsar.client.") && value != null) {
                    String configKey = key.substring("pulsar.client.".length());
                    // Add more configuration options as needed
                }
            });

            this.client = clientBuilder.build();

            if (authToken != null && !authToken.isEmpty()) {
                this.admin = PulsarAdmin.builder()
                        .serviceHttpUrl(adminUrl)
                        .authentication(
                                "org.apache.pulsar.client.impl.auth.AuthenticationToken",
                                authToken
                        )
                        .build();
            } else {
                this.admin = PulsarAdmin.builder()
                        .serviceHttpUrl(adminUrl)
                        .build();
            }

            // Ensure the default namespace exists
            try {
                if (!admin.tenants().getTenants().contains(DEFAULT_TENANT)) {
                    admin.tenants().createTenant(DEFAULT_TENANT,
                            TenantInfo.builder().build());
                }
                if (!admin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
                    admin.namespaces().createNamespace(DEFAULT_NAMESPACE);
                }
            } catch (PulsarAdminException e) {
                log.warn("Error checking/creating default namespace", e);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize Pulsar driver", e);
        }
    }

    private synchronized org.apache.pulsar.client.api.v5.PulsarClient getOrCreateScalableClient() throws IOException {
        if (scalableClient == null) {
            var builder = org.apache.pulsar.client.api.v5.PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .operationTimeout(Duration.ofSeconds(30))
                    .connectionPolicy(ConnectionPolicy.builder().connectionTimeout(Duration.ofSeconds(30)).build());
            if (authToken != null && !authToken.isEmpty()) {
                builder.authentication(AuthenticationFactory.token(authToken));
            }
            scalableClient = builder.build();
        }
        return scalableClient;
    }

    /** Scalable topics are addressed by their topic:// name, classic topics by a short or persistent:// name */
    private static boolean isScalableTopic(String topicName) {
        return topicName.startsWith("topic://");
    }

    /**
     * Creates a new Pulsar topic with the specified name and partition count.
     *
     * @param topicName Name of the topic to create
     * @param partitions Number of partitions for the topic, or initial segments for a scalable topic
     * @param configs Topic configuration; unsupported by the Pulsar driver
     * @throws RuntimeException if topic creation fails
     */
    @Override
    public void createTopic(String topicName, int partitions, Map<String, String> configs) {
        if (configs != null && !configs.isEmpty()) {
            throw new IllegalArgumentException("Topic configs are not supported by the Pulsar driver");
        }
        try {
            String fullTopicName = getFullTopicName(topicName);
            boolean scalable = isScalableTopic(topicName);
            if (scalable) {
                admin.scalableTopics().createScalableTopic(fullTopicName, partitions);
            } else if (partitions > 1) {
                admin.topics().createPartitionedTopic(fullTopicName, partitions);
            } else {
                admin.topics().createNonPartitionedTopic(fullTopicName);
            }
            log.info("Created Pulsar topic: {} with {} {}", fullTopicName, partitions,
                    scalable ? "initial segments" : "partitions");
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 409) {
                log.info("Topic already exists, continuing");
            } else {
                throw new RuntimeException("Failed to create topic: " + topicName, e);
            }
        }
    }

    /**
     * Deletes a Pulsar topic and all its associated resources.
     *
     * @param topicName Name of the topic to delete
     * @throws RuntimeException if topic deletion fails
     */
    @Override
    public void deleteTopic(String topicName) {
        try {
            String fullTopicName = getFullTopicName(topicName);
            if (isScalableTopic(topicName)) {
                // Also removes the subscriptions and every segment of the topic
                admin.scalableTopics().deleteScalableTopic(fullTopicName, true);
                log.info("Deleted Pulsar scalable topic: {}", fullTopicName);
                return;
            }

            // Check if topic is partitioned (partitions > 0 means partitioned)
            boolean isPartitioned = false;
            try {
                int partitions = admin.topics().getPartitionedTopicMetadata(fullTopicName).partitions;
                isPartitioned = partitions > 0;
            } catch (PulsarAdminException.NotFoundException e) {
                // Topic doesn't exist
            }

            deleteSubscriptions(fullTopicName, isPartitioned);

            if (isPartitioned) {
                admin.topics().deletePartitionedTopic(fullTopicName, true);
            } else {
                admin.topics().delete(fullTopicName, true);
            }
            log.info("Deleted Pulsar topic: {}", fullTopicName);
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.NotFoundException) {
                log.info("Topic does not exist, nothing to delete");
            } else {
                throw new RuntimeException("Failed to delete topic: " + topicName, e);
            }
        }
    }

    private void deleteSubscriptions(String fullTopicName, boolean isPartitioned) {
        try {
            List<String> subscriptions = admin.topics().getSubscriptions(fullTopicName);
            for (String subscription : subscriptions) {
                try {
                    admin.topics().deleteSubscription(fullTopicName, subscription, true);
                    log.info("Deleted subscription {} on topic {}", subscription, fullTopicName);
                } catch (PulsarAdminException e) {
                    log.warn("Failed to delete subscription {} on topic {}", subscription, fullTopicName, e);
                }
            }
        } catch (PulsarAdminException e) {
            log.warn("Failed to list subscriptions for topic {}", fullTopicName, e);
        }
    }

    /**
     * Creates a new producer with at-least-once delivery guarantees.
     *
     * @param topicName Target topic for the producer
     * @param configs Producer-specific configurations
     * @param messageSize Total size in bytes of each message value
     * @return A configured ProofProducer instance
     */
    @Override
    public ProofProducer createProducer(String topicName, Map<String, Object> configs, int messageSize) {
        try {
            if (isScalableTopic(topicName)) {
                return new PulsarScalableProofProducer(getOrCreateScalableClient(), topicName, messageSize);
            }
            return new PulsarAtLeastOnceProofProducer(client, topicName, configs, messageSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new consumer with the specified message listener.
     *
     * @param topicName Topic to consume from
     * @param configs Consumer-specific configurations
     * @param listener Callback for processing consumed messages
     * @return A configured ProofConsumer instance
     */
    @Override
    public ProofConsumer createConsumer(String topicName, int partitionCount, long consumeDelayMs,
                Map<String, Object> configs, MessageListener listener) {
        try {
            String consumerName = UUID.randomUUID().toString();
            if (isScalableTopic(topicName)) {
                return createScalableConsumer(consumerName, topicName, consumeDelayMs, configs, listener);
            }
            return new PulsarAtLeastOnceProofConsumer(consumerName, client, topicName, configs, consumeDelayMs,
                    listener, admin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** The offload conditions only work on classic topics. */
    private ProofConsumer createScalableConsumer(String consumerName, String topicName, long consumeDelayMs,
            Map<String, Object> configs, MessageListener listener) throws IOException {
        if (OffloadCondition.getOffloadCondition(admin, topicName, configs).isPresent()) {
            throw new IllegalArgumentException("Offload conditions are not supported for scalable topics");
        }
        int checkpointRestartMessages = Integer.parseInt(
                String.valueOf(configs.getOrDefault("pulsar.checkpoint.restart.messages", 0)));
        if (checkpointRestartMessages < 0) {
            throw new IllegalArgumentException("pulsar.checkpoint.restart.messages must not be negative");
        }
        return new PulsarScalableProofConsumer(consumerName, getOrCreateScalableClient(), topicName,
                (String) configs.getOrDefault(PULSAR_CONSUMER_TYPE, "stream"), consumeDelayMs, listener,
                checkpointRestartMessages);
    }

    /**
     * Constructs the fully qualified topic name: a short name is a classic topic in the default namespace.
     *
     * @param topicName The short or fully qualified topic name
     * @return The fully qualified topic name
     */
    private String getFullTopicName(String topicName) {
        return topicName.contains("://") ? topicName : "persistent://" + DEFAULT_NAMESPACE + "/" + topicName;
    }

    /**
     * Closes the Pulsar client and admin resources.
     */
    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
            if (scalableClient != null) {
                scalableClient.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            log.error("Error closing Pulsar resources", e);
        }
    }
}
