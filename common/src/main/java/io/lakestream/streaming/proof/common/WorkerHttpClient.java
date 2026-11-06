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
package io.lakestream.streaming.proof.common;

import static java.net.HttpURLConnection.HTTP_OK;

import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import io.lakestream.streaming.proof.common.records.PublishLatencyWindowSnapshot;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.asynchttpclient.AsyncHttpClient;

/**
 * HTTP client for interacting with the Streaming Proof Worker service.
 * This client provides asynchronous methods to manage producers and consumers
 * through REST API calls to the worker nodes.
 *
 * <p>The client uses AsyncHttpClient for non-blocking HTTP operations and returns
 * CompletableFuture for all operations to support asynchronous programming patterns.
 *
 * <p>Example usage:
 * <pre>{@code
 * AsyncHttpClient asyncClient = Dsl.asyncHttpClient();
 * WorkerHttpClient client = new WorkerHttpClient(asyncClient, "http://localhost:8088");
 *
 * // Start producers
 * NewProducers producers = NewProducers.builder()
 *     .id("test-producers")
 *     .producers(10)
 *     .build();
 * client.startProducers(producers)
 *     .thenCompose(v -> client.producerCheckpoint("test-producers"))
 *     .thenAccept(System.out::println);
 * }</pre>
 *
 * @see Worker
 * @see ProofProducers
 * @see ProofConsumers
 */
public class WorkerHttpClient {

    private final AsyncHttpClient client;
    private final String baseUrl;

    /**
     * Creates a new WorkerHttpClient instance.
     *
     * @param client The AsyncHttpClient instance to use for HTTP requests
     * @param workerBaseUrl The base URL of the worker service (e.g., "http://localhost:8088")
     */
    public WorkerHttpClient(AsyncHttpClient client, String workerBaseUrl) {
        this.client = client;
        this.baseUrl = workerBaseUrl;
    }

    /**
     * Starts a group of producers on the worker node.
     *
     * @param newProducers Configuration for the new producers to start
     * @return A CompletableFuture that completes when the producers are started
     * @throws Exception if there's an error serializing the request
     */
    public CompletableFuture<Void> startProducers(NewProducers newProducers) throws Exception {
        return client.preparePost(baseUrl + Util.START_PRODUCER)
                .setBody(Util.JSON_WRITER.writeValueAsBytes(newProducers))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to start producers: " + response.getStatusCode());
                    }
                });
    }

    /**
     * Stops a group of producers identified by the given ID.
     *
     * @param id The unique identifier of the producer group to stop
     * @return A CompletableFuture that completes when the producers are stopped
     * @throws Exception if there's an error preparing the request
     */
    public CompletableFuture<Void> stopProducers(String id) throws Exception {
        return client.preparePost(baseUrl + Util.STOP_PRODUCER.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to stop producers: " + response.getStatusCode());
                    }
                });
    }

    /**
     * Retrieves the checkpoint information for a group of producers.
     *
     * @param id The unique identifier of the producer group
     * @return A CompletableFuture that completes with the checkpoint information
     * @throws Exception if there's an error preparing the request or deserializing the response
     */
    public CompletableFuture<ProducerCheckpoint> producerCheckpoint(String id) throws Exception {
        return client.prepareGet(baseUrl + Util.PRODUCER_CHECKPOINTS.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to get producer checkpoint: " + response.getStatusCode());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(), ProducerCheckpoint.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Starts a group of consumers on the worker node.
     *
     * @param newConsumers Configuration for the new consumers to start
     * @return A CompletableFuture that completes when the consumers are started
     * @throws Exception if there's an error serializing the request
     */
    public CompletableFuture<Void> startConsumers(NewConsumers newConsumers) throws Exception {
        return client.preparePost(baseUrl + Util.START_CONSUMER)
                .setBody(Util.JSON_WRITER.writeValueAsBytes(newConsumers))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to start consumers: " + response.getStatusCode());
                    }
                });
    }

    /**
     * Stops a group of consumers identified by the given ID and removes the
     * associated producer group from the registry.
     *
     * @param id The unique identifier of the consumer group to stop and remove
     * @return A CompletableFuture that completes when the consumers are stopped
     * @throws Exception if there's an error preparing the request
     */
    public CompletableFuture<Void> stopAndRemoveConsumers(String id) throws Exception {
        return client.preparePost(baseUrl + Util.STOP_CONSUMER.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to stop and remove consumers: " + response.getStatusCode());
                    }
                });
    }

    /**
     * Retrieves the checkpoint information for a group of consumers.
     * Workers trim consumed ranges at or below each watermark before returning the checkpoint.
     *
     * @param id The unique identifier of the consumer group
     * @param watermarks Map of key to high watermark; workers trim ranges at or below
     *                   each watermark. Pass empty map to skip trimming.
     * @return A CompletableFuture that completes with the checkpoint information
     * @throws Exception if there's an error preparing the request or deserializing the response
     */
    public CompletableFuture<ConsumerCheckPoint> consumerCheckpoint(String id, Map<String, Long> watermarks)
            throws Exception {
        return client.preparePost(baseUrl + Util.CONSUMER_CHECKPOINTS.replace("{id}", id))
                .setBody(Util.JSON_WRITER.writeValueAsBytes(watermarks))
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to get consumer checkpoint: "
                                + response.getStatusCode());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(),
                                ConsumerCheckPoint.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public CompletableFuture<ProofWorkerMetricsSnapshot> metrics(String id) throws Exception {
        return client.prepareGet(baseUrl + Util.WORKER_METRICS.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to get worker metrics: " + response.getStatusCode());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(), ProofWorkerMetricsSnapshot.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Fetches windowed metrics from the worker.  Latency samples only cover the
     * interval since the previous windowed read; counters remain cumulative.
     */
    public CompletableFuture<ProofWorkerMetricsSnapshot> metricsWindowed(String id) throws Exception {
        return client.prepareGet(baseUrl + Util.WORKER_METRICS_WINDOWED.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException(
                                "Failed to get windowed worker metrics: " + response.getStatusCode());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(), ProofWorkerMetricsSnapshot.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
    /** Reads one closed fixed bucket without consuming the chart's reset-on-read samples. */
    public CompletableFuture<PublishLatencyWindowSnapshot> publishLatencyWindow(String id, long windowId)
            throws Exception {
        return client.prepareGet(baseUrl + Util.WORKER_PUBLISH_LATENCY_WINDOW
                        .replace("{id}", id).replace("{windowId}", Long.toString(windowId)))
                .execute().toCompletableFuture().thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new IllegalStateException("Failed to get publish latency window: "
                                + response.getStatusCode());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(),
                                PublishLatencyWindowSnapshot.class);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

}
