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

import io.lakestream.streaming.proof.common.ProofProducer;

/**
 * A record representing a request to create new producers in the streaming proof system.
 * This immutable record encapsulates all necessary information to initialize and start
 * a group of producers for performance and reliability testing.
 *
 * <p>The record is used by the {@link io.lakestream.streaming.proof.Worker} to create
 * and manage producer instances that will generate test messages with specific patterns
 * and rates.
 *
 * <p>When {@code transactional} is true, each producer will:
 * <ul>
 *   <li>Write messages to input topic ({topic}_transactional)</li>
 *   <li>Start an embedded transactional processor</li>
 *   <li>The processor will read from input topic and write to output topic ({topic}) atomically</li>
 *   <li>Consumers will read from output topic for exactly-once verification</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewProducers producers = new NewProducers(
 *     "proof-123",      // unique identifier
 *     "test-topic",     // topic name (output topic)
 *     4,                // number of producer instances
 *     100,              // number of unique message keys
 *     1000,             // message rate (msgs/sec)
 *     "kafka",          // driver name
 *     kafkaDriver,      // messaging system driver
 *     true              // enable transactional processing
 * );
 * }</pre>
 *
 * @param id A unique identifier for this group of producers, typically matching
 *          the proof test ID they belong to
 * @param topic The base topic name. If transactional=true, producers write to {topic}_transactional
 *             and consumers read from {topic}. If transactional=false, both write to and read from {topic}
 * @param producers The number of producer instances to create, enabling parallel
 *                 message production for higher throughput
 * @param keys The number of unique message keys to use across all producers,
 *            affecting the distribution and partitioning of messages
 * @param msgRate The target message production rate in messages per second,
 *               distributed across all producer instances
 * @param messageSize The total size in bytes of each message value, including the
 *                    8-byte sequence number. Unlike the counts above, this is not
 *                    divided across workers.
 * @param driverName The name of the messaging system driver
 * @param driver The messaging system driver configuration that specifies how to
 *              create and configure the producer instances
 * @param transactional If true, enables exactly-once processing using embedded transactional processors
 * @param publishLatencyWindowSeconds Fixed latency bucket length; 0 disables collection
 * @param publishLatencyP99LimitMillis Threshold used to count slow acknowledgments
 *
 * @see Driver
 * @see ProofProducer
 * @see io.lakestream.streaming.proof.ProofProducers
 */
public record NewProducers(String id,
                           String topic,
                           int producers,
                           int keys,
                           int msgRate,
                           int messageSize,
                           String driverName,
                           Driver driver,
                           boolean transactional,
                           int publishLatencyWindowSeconds,
                           long publishLatencyP99LimitMillis) {
    public NewProducers(String id, String topic, int producers, int keys, int msgRate,
                        int messageSize, String driverName, Driver driver, boolean transactional,
                        int publishLatencyWindowSeconds) {
        this(id, topic, producers, keys, msgRate, messageSize, driverName, driver, transactional,
                publishLatencyWindowSeconds, 0);
    }

    public NewProducers(String id, String topic, int producers, int keys, int msgRate,
                        int messageSize, String driverName, Driver driver, boolean transactional) {
        this(id, topic, producers, keys, msgRate, messageSize, driverName, driver, transactional, 0, 0);
    }
}
