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

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.NewProducers;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP request handler for the Streaming Proof Worker service.
 * This class manages REST API endpoints for controlling producer and consumer
 * instances on a worker node, providing operations for lifecycle management
 * and monitoring.
 *
 * <p>The handler supports the following operations:
 * <ul>
 *   <li>Starting and stopping producer groups</li>
 *   <li>Starting and stopping consumer groups</li>
 *   <li>Retrieving producer and consumer checkpoints</li>
 * </ul>
 *
 * <p>All operations are identified by a proof ID, allowing multiple test
 * scenarios to run concurrently on the same worker node.
 *
 * <p>Example usage:
 * <pre>{@code
 * Javalin app = Javalin.create();
 * new WorkerHandler(app);
 * app.start(8088);
 *
 * // REST API endpoints:
 * // POST /producers/start - Start producers with configuration
 * // POST /consumers/start - Start consumers with configuration
 * // GET /producers/checkpoints/{id} - Get producer statistics
 * // POST /consumers/checkpoints/{id} - Get consumer statistics (with watermark trimming)
 * // POST /producers/stop/{id} - Stop producer group
 * // POST /consumers/stop/{id} - Stop consumer group
 * }</pre>
 *
 * @see Worker
 * @see ProofProducers
 * @see ProofConsumers
 */
@Slf4j
public class WorkerHandler {

    /** The worker instance that manages producer and consumer groups */
    private final Worker worker;

    /**
     * Creates a new WorkerHandler and registers all REST API endpoints.
     *
     * @param app The Javalin application instance to register endpoints with
     */
    public WorkerHandler(Javalin app, Worker worker) {
        this.worker = worker;
        app.post(Util.START_PRODUCER, this::handleStartProducer);
        app.post(Util.START_CONSUMER, this::handleStartConsumer);
        app.get(Util.PRODUCER_CHECKPOINTS, this::handleProducerCheckpoints);
        app.post(Util.CONSUMER_CHECKPOINTS, this::handleConsumerCheckpoints);
        app.get(Util.CONSUMER_CHECKPOINTS_DETAILS, this::handleConsumerCheckpointsDetails);
        app.get(Util.WORKER_METRICS, this::handleWorkerMetrics);
        app.get(Util.WORKER_METRICS_WINDOWED, this::handleWorkerMetricsWindowed);
        app.get(Util.WORKER_PUBLISH_LATENCY_WINDOW, ctx -> {
            long windowId = ctx.pathParamAsClass("windowId", Long.class).get();
            Object snapshot = worker.publishLatencyWindowSnapshot(ctx.pathParam("id"), windowId);
            if (snapshot == null) {
                ctx.status(404);
            } else {
                ctx.result(Util.JSON_WRITER.writeValueAsString(snapshot));
            }
        });
        app.post(Util.STOP_PRODUCER, this::handleStopProducer);
        app.post(Util.STOP_CONSUMER, this::handleStopConsumer);
    }

    /**
     * Handles requests to start a new producer group.
     * Deserializes producer configuration from the request body and
     * delegates to the worker to create and start the producers.
     *
     * @param ctx The Javalin context containing the request details
     * @throws Exception if there's an error processing the request
     */
    private void handleStartProducer(Context ctx) throws Exception {
        NewProducers newProducers = Util.JSON_MAPPER.readValue(ctx.body(), NewProducers.class);
        worker.startProducers(newProducers);

    }

    /**
     * Handles requests to start a new consumer group.
     * Deserializes consumer configuration from the request body and
     * delegates to the worker to create and start the consumers.
     *
     * @param ctx The Javalin context containing the request details
     * @throws Exception if there's an error processing the request
     */
    private void handleStartConsumer(Context ctx) throws Exception {
        NewConsumers newConsumers = Util.JSON_MAPPER.readValue(ctx.body(), NewConsumers.class);
        worker.startConsumers(newConsumers);
    }

    /**
     * Handles requests to retrieve producer checkpoints.
     * Returns statistics and sequence information for the specified producer group.
     *
     * @param ctx The Javalin context containing the proof ID in path parameters
     * @throws Exception if there's an error processing the request
     */
    private void handleProducerCheckpoints(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        ProducerCheckpoint checkPoint = worker.producerCheckPoint(proofID);
        ctx.result(Util.JSON_WRITER.writeValueAsString(checkPoint));
    }

    /**
     * Handles requests to retrieve consumer checkpoints.
     * Returns statistics and sequence information for the specified consumer group.
     *
     * @param ctx The Javalin context containing the proof ID in path parameters
     * @throws Exception if there's an error processing the request
     */
    private void handleConsumerCheckpoints(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        String body = ctx.body();
        if (body != null && !body.isBlank()) {
            @SuppressWarnings("unchecked")
            Map<String, Long> watermarks = Util.JSON_MAPPER.readValue(body,
                    Util.JSON_MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Long.class));
            if (!watermarks.isEmpty()) {
                worker.applyConsumerWatermarks(proofID, watermarks);
            }
        }
        ConsumerCheckPoint checkPoint = worker.consumerCheckPoint(proofID);
        ctx.result(Util.JSON_WRITER.writeValueAsString(checkPoint));
    }

    private void handleConsumerCheckpointsDetails(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        Map<String, ConsumerCheckPoint> checkPoint = worker.consumerCheckPointDetails(proofID);
        ctx.result(Util.JSON_WRITER.writeValueAsString(checkPoint));
    }

    private void handleWorkerMetrics(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        ProofWorkerMetricsSnapshot metricsSnapshot = worker.metricsSnapshot(proofID);
        ctx.result(Util.JSON_WRITER.writeValueAsString(metricsSnapshot));
    }

    private void handleWorkerMetricsWindowed(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        ProofWorkerMetricsSnapshot metricsSnapshot = worker.windowedMetricsSnapshot(proofID);
        ctx.result(Util.JSON_WRITER.writeValueAsString(metricsSnapshot));
    }

    /**
     * Handles requests to stop a producer group.
     * Stops all producers in the group identified by the proof ID.
     *
     * @param ctx The Javalin context containing the proof ID in path parameters
     * @throws Exception if there's an error processing the request
     */
    private void handleStopProducer(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        worker.stopProducers(proofID);
    }

    /**
     * Handles requests to stop a consumer group and remove the associated producer.
     * Stops all consumers in the group identified by the proof ID.
     *
     * @param ctx The Javalin context containing the proof ID in path parameters
     * @throws Exception if there's an error processing the request
     */
    private void handleStopConsumer(Context ctx) throws Exception {
        String proofID = ctx.pathParam("id");
        worker.stopAndRemoveConsumers(proofID);
    }
}
