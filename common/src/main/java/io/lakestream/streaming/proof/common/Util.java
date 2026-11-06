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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Utility class providing common functionality for the streaming proof system.
 * This class contains JSON handling utilities, supported feature definitions,
 * and REST API endpoint definitions used throughout the system.
 *
 * <p>The class is marked with {@link UtilityClass} to ensure it cannot be instantiated
 * and all methods must be static.
 */
@UtilityClass
public class Util {

    /**
     * Configured ObjectMapper instance for JSON serialization/deserialization.
     * This mapper is configured to handle unknown enum values by using default values
     * instead of throwing exceptions.
     */
    public static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .registerModule(new SimpleModule().addDeserializer(Pair.class, new PairDeserializer()))
            .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true);

    /**
     * Preconfigured ObjectWriter instance that outputs JSON with pretty printing.
     * This writer is thread-safe and can be used across multiple threads.
     */
    public static final ObjectWriter JSON_WRITER = JSON_MAPPER.writer();

    /**
     * Set of supported proof features that can be validated by the system.
     * Currently supports:
     * <ul>
     *   <li>{@code at_least_once}: Validates at-least-once message delivery semantics</li>
     *   <li>{@code ordering}: Validates message ordering guarantees</li>
     *   <li>{@code exactly_once}: Validates exactly-once semantics using transactional processors</li>
     * </ul>
     */
    public static final Set<String> SUPPORTED_PROOF_FEATS = Set.of("at_least_once", "ordering", "exactly_once");

    /**
     * REST API endpoints for proof management operations.
     * These endpoints handle the lifecycle of proof tests including creation,
     * monitoring, stopping, and deletion.
     */
    public static final String CREATE_PROOF = "/proofs";
    /** Endpoint to retrieve a specific proof test by ID */
    public static final String GET_PROOF = "/proofs/{id}";
    public static final String GET_PROOF_DETAILS = "/proofs/{id}/details";
    public static final String GET_PROOF_REPORT = "/proofs/{id}/report";
    /** Endpoint to stop a running proof test */
    public static final String STOP_PROOF = "/proofs/{id}/stop";
    /** Endpoint to delete a proof test and its resources */
    public static final String DELETE_PROOF = "/proofs/{id}";
    /** Endpoint to list all proof tests */
    public static final String LIST_PROOFS = "/proofs";

    /**
     * REST API endpoints for configuration management.
     * These endpoints handle system-wide configuration settings including
     * worker configurations and driver settings.
     */
    public static final String PUT_CONFIG = "/configs";
    /** Endpoint to retrieve current system configuration */
    public static final String GET_CONFIG = "/configs";
    /** Endpoint to delete configuration settings */
    public static final String DELETE_CONFIG = "/configs";

    /**
     * REST API endpoints for producer management.
     * These endpoints handle producer lifecycle and monitoring, including
     * starting producers, retrieving checkpoints, and stopping producers.
     */
    public static final String START_PRODUCER = "/producers/start";
    /** Endpoint to retrieve producer checkpoints for verification */
    public static final String PRODUCER_CHECKPOINTS = "/producers/checkpoints/{id}";
    /** Endpoint to stop a running producer */
    public static final String STOP_PRODUCER = "/producers/stop/{id}";

    /**
     * REST API endpoints for consumer management.
     * These endpoints handle consumer lifecycle and monitoring, including
     * starting consumers, retrieving checkpoints, and stopping consumers.
     */
    public static final String START_CONSUMER = "/consumers/start";
    /** Endpoint to retrieve consumer checkpoints for verification */
    public static final String CONSUMER_CHECKPOINTS = "/consumers/checkpoints/{id}";
    public static final String CONSUMER_CHECKPOINTS_DETAILS = "/consumers/checkpoints/{id}/details";
    public static final String WORKER_METRICS = "/metrics/{id}";
    /** Endpoint to retrieve windowed worker metrics with reset-on-read latency samples */
    public static final String WORKER_METRICS_WINDOWED = "/metrics/{id}/windowed";
    /** Independent fixed publish latency buckets, addressed by window ID. */
    public static final String WORKER_PUBLISH_LATENCY_WINDOW = "/metrics/{id}/publish-latency/{windowId}";
    /** Endpoint to stop a running consumer */
    public static final String STOP_CONSUMER = "/consumers/stop/{id}";

}
