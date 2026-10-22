# Design Document: Streaming Systems Correctness Verification Framework

- **Author:** Penghui Li
- **Date:** 2025-02-06
- **Status:** Implemented

## 1. Abstract

This document outlines the design for a framework dedicated to verifying the correctness guarantees (e.g., at-least-once, exactly-once, ordering) of streaming systems like Apache Kafka and Apache Pulsar. The framework employs a distributed architecture with producers and consumers generating and validating sequenced data streams across multiple keys. It provides real-time monitoring, checkpointing for state comparison, and detailed reporting of any detected violations (message loss, duplicates, out-of-order delivery).

## 2. Goals

*   Provide a reliable and scalable method to verify message delivery guarantees (at-least-once, exactly-once) and ordering for various streaming systems.
*   Support verification for Kafka-compatible and Pulsar systems through a pluggable driver architecture.
*   Enable configuration of workload parameters (producers, consumers, keys, message rate, test duration).
*   Offer real-time monitoring and reporting of verification status and detected errors via REST APIs.
*   Implement a checkpointing mechanism to compare producer and consumer states for correctness validation.
*   Provide clear evidence (e.g., sequence numbers, keys) for any detected violations.
*   Support deployment in containerized environments (Docker, Kubernetes).

## 3. Non-Goals

*   Performance benchmarking of the streaming systems (focus is on correctness, not throughput/latency measurement).
*   End-to-end application-level correctness verification (focus is on the streaming system's guarantees).
*   Automated failure injection (though designed to work alongside chaos testing tools).
*   Verification of complex stream processing logic beyond basic delivery guarantees.

## 4. Background and Motivation

Verifying the correctness of distributed streaming systems, especially under failure conditions or specific configurations, is challenging. Guarantees like "at-least-once", "exactly-once", and "ordering" are critical for many applications but difficult to confirm empirically. Existing testing methods often lack the scale, automation, or detailed reporting needed for thorough validation.

This framework originated from the need for a robust way to verify streaming-system correctness. It provides a standardized, scalable, and automated solution for continuously checking these guarantees in systems such as Apache Kafka, Apache Pulsar, and other compatible platforms, particularly before production deployment or during chaos engineering experiments.

## 5. Proposed Solution: High-Level Design

The core idea is to generate uniquely sequenced messages for distinct keys and verify their reception and order at the consumer side.

### 5.1. Architecture Components

*   **Coordinator:**
    *   Manages the overall verification lifecycle (start, stop, configuration).
    *   Orchestrates Worker nodes.
    *   Assigns producer/consumer tasks and key ranges to Workers.
    *   Aggregates checkpoints and statistics from Workers.
    *   Performs cross-worker state comparison for verification.
    *   Provides a central REST API for control and reporting.
*   **Worker:**
    *   Runs on separate nodes/containers for scalability.
    *   Registers with the Coordinator.
    *   Executes producer and/or consumer tasks as assigned by the Coordinator.
    *   Uses specific `ProofDriver` implementations to interact with the target streaming system.
    *   Tracks local state (last produced/consumed sequence per key, detected errors).
    *   Provides local checkpoints and statistics to the Coordinator via REST API.
*   **ProofDriver:**
    *   An interface ProofDriver.java defining interactions with a specific streaming system (e.g., creating producers/consumers, sending/receiving messages).

### 5.2. Key Concepts

*   **Key:** A partition identifier used to group related messages. Ordering is typically guaranteed within a key/partition in many streaming systems.
*   **Sequence Number (Seq):** A monotonically increasing number assigned to each message within a specific key. Used to detect loss, duplicates, and out-of-order messages.
*   **Checkpoint:** A snapshot of the state (e.g., last sequence number produced/consumed per key, error counts) taken periodically by producers and consumers. The Coordinator aggregates these for verification.

### 5.3. Workflow

1.  **Configuration:** User defines the test parameters (driver, features, producers, consumers, keys, rate, etc.) and system configurations (worker endpoints, driver settings) via the Coordinator API (`POST /proofs`, `PUT /configs`).
2.  **Initialization:** Coordinator starts, loads configuration, and potentially creates the topic in the target system. Workers start and register with the Coordinator.
3.  **Task Distribution:** Coordinator receives a `POST /proofs` request. It calculates key distribution, assigns producer/consumer tasks to registered Workers, and instructs them to start via Worker APIs (`POST /producer/start`, `POST /consumer/start`).
4.  **Data Generation & Consumption:**
    *   Producers (on Workers) generate messages with `(Key, Seq)` pairs and send them via the configured `ProofDriver`. They track the last sent `Seq` for each assigned `Key`.
    *   Consumers (on Workers) receive messages via the `ProofDriver`. They track the last *valid* `Seq` received for each `Key` and record any duplicates, out-of-order messages, or calculate potential misses based on sequence gaps.
5.  **Checkpointing & Verification:**
    *   Coordinator periodically triggers checkpoints (`GET /producer/checkpoint/{id}`, `POST /consumer/checkpoint/{id}` from Workers). The consumer checkpoint request includes high watermarks for memory trimming.
    *   Coordinator aggregates producer checkpoints to determine the expected state (highest `Seq` produced per `Key`).
    *   Coordinator aggregates consumer checkpoints (highest *valid* `Seq` consumed per `Key`, error counts).
    *   Coordinator compares the aggregated producer state ("InCheck" state) with the aggregated consumer state over time. It uses a configurable `timeout` to determine if messages produced in a checkpoint window are eventually consumed correctly. If sequences remain unverified after the timeout, a timeout error is flagged.
6.  **Reporting:** User queries the Coordinator API (`GET /proofs/{id}`) to get real-time summary statistics (total verified, errors, duplicates, missed, timeouts) and detailed checkpoint information.
7.  **Termination:** User stops the test via the Coordinator API (`PUT /proofs/{id}/stop`), which propagates stop commands to the Workers (`POST /producer/stop/{id}`, `POST /consumer/stop/{id}`).

### 5.4. Verification Mechanisms
The streaming-proof framework implements robust verification mechanisms to ensure the correctness of streaming systems. These mechanisms focus on detecting missed messages and duplicated messages, which are critical for validating delivery guarantees.

#### 5.4.1. Checkpoint-Based Verification
The framework uses a checkpoint model that tracks message sequences for both producers and consumers:

- Producer Checkpoints : Track the latest sequence numbers published for each message key
- Consumer Checkpoints : Maintain sequence ranges of consumed messages and track anomalies
#### 5.4.2. Detecting Missed Messages
Missed message detection works by identifying gaps between consecutive message ranges received by consumers. When a gap is detected between the end of one sequence range and the start of another, those missing sequence numbers represent missed messages.

If a consumer receives these sequence ranges:

- Range 1: [1-5]
- Range 2: [10-15]
The system identifies a gap: [6-9]

This indicates messages with sequence numbers 6, 7, 8, and 9 were missed.

#### 5.4.3. Detecting Duplicated Messages
Duplicate detection works by identifying:

1. Messages with sequence numbers that have already been processed
2. Overlapping sequence ranges in the consumer's record

If a consumer has already processed sequence number 42 for key "A", and then receives another message with sequence 42 for the same key, it's identified as a duplicate.

For range analysis, if these ranges exist:

- Range 1: [1-10]
- Range 2: [5-15]
The system identifies an overlap: [5-10]

This indicates 6 duplicate messages (sequence numbers 5, 6, 7, 8, 9, 10).

#### 5.4.4. Sequence Range Optimization on consumer
To improve verification efficiency, the framework implements:

1. Range Merging : Adjacent and overlapping sequence ranges are merged to create a more compact representation
2. Trimming : The trim() method optimizes sequence ranges by combining adjacent ranges where the end of one range + 1 equals the start of the next range

#### 5.4.5. Watermark-Based Memory Management
The framework uses a high-watermark mechanism to bound memory usage during long-running proofs. Without watermarks, consumers accumulate sequence ranges indefinitely.

**How it works:**
1. The coordinator computes **high watermarks** per key — the highest contiguous sequence number that has been fully verified
2. When the coordinator fetches consumer checkpoints, it sends the current watermarks in the request body (POST)
3. Workers apply the watermarks before returning the checkpoint: ranges with `end <= watermark` are fully removed, ranges spanning the watermark are trimmed to start at `watermark + 1`
4. The coordinator then computes new watermarks from the trimmed checkpoint, using the previous watermarks as the contiguity starting point

This creates a sliding window that keeps only unverified ranges in memory, regardless of how long the proof runs. The mechanism applies to all subscription types (Kafka consumer groups, Pulsar Failover, Key_Shared, and Shared).

#### 5.4.6. Shared Subscription Verification (Pulsar)
Pulsar Shared subscriptions distribute messages round-robin across consumers. Unlike Failover or Key_Shared, a single key's messages may be consumed by any consumer, so per-key ordering is **not guaranteed**.

**Detection:** The framework automatically detects Shared mode when `subscriptionType: "Shared"` is set in the Pulsar consumer configuration.

**Worker-side differences:**
- `ProofConsumerTask` operates in shared mode: `onMessage()` records consumed ranges without ordering analysis — gaps and out-of-order arrival are expected, and `writeDupsOrOutOrder` tracking is disabled
- Duplicate detection still works via `SeqRange.duplicated` when a message's sequence falls within an existing range

**Coordinator-side verification:**
Instead of checking `getLastSeq(key) >= producerSeq` (which only confirms the highest sequence arrived), Shared verification uses **high-watermark contiguity checking**:

1. Merge consumer checkpoints from all workers per key
2. Compute high watermarks: walk the merged, sorted ranges to find the highest contiguous sequence starting from seq 0 (or from the previous watermark after trimming)
3. For each key, verify `highWatermark >= producerLastSeq`
4. If all keys pass, verification succeeds; if timeout is exceeded, record failure

**Summary reporting for Shared mode:**
- `verified`: sum of `(highWatermark + 1)` across all keys (sequences are 0-based)
- `duplicates`: detected via existing `SeqRange.duplicated` overlap counting
- `missed`: sequences up to the producer checkpoint under verification that are still not consumed once it timed out (or once the final wait is over). Gaps above it are messages still on their way to another consumer, so they are not counted
- `outOfOrders`: always 0 (expected for Shared, not reported)

**API example:**
```json
{
  "name": "pulsar-shared-test",
  "driver": "pulsar_driver",
  "features": ["at_least_once"],
  "topic": "test-topic",
  "partitions": 1,
  "producers": 2,
  "consumers": 4,
  "pulsar": {
    "consumerConfig": {
      "subscriptionType": "Shared"
    }
  }
}
```

#### 5.4.7. Handling Partition Reassignment
The framework handles partition reassignment to different consumers through timestamp-based sequence range tracking:
- **Timestamp-Based Range Creation** : When a consumer processes messages, it creates sequence ranges with timestamps. Each range is identified by the timestamp when it was created.
- **Sequence Range Continuity** : When a consumer starts processing messages from a newly assigned partition, it creates new sequence ranges with new timestamps. This allows the system to:
  - Track which consumer processed which message ranges
  - Identify when partition ownership changed
  - Detect potential duplicates or missed messages during reassignment
- **Checkpoint Aggregation** : During verification, the coordinator collects checkpoints from all consumers and merges them based on timestamps:
  - Ranges are sorted chronologically using the timestamp keys in the SortedMap
  - The system can identify when a partition was reassigned by analyzing the timestamp gaps
  - Overlapping ranges from different consumers indicate potential duplicates during rebalancing
- **Verification During Rebalancing** : The timestamp-based approach ensures that even when partitions are reassigned:
  - Message sequence integrity can be verified across consumer instances
  - Duplicates processed by different consumers are detected through overlapping ranges
  - Missed messages during rebalancing are identified through sequence gaps
#### 5.4.8. Verification Process
The verification process follows these steps:
1. Producers publish messages with sequential values for each key
2. Consumers receive messages and track sequence ranges
3. The system periodically collects producer and consumer checkpoints
4. Checkpoints are analyzed to detect:
   - Messages that were published but never consumed (missed messages)
   - Messages that were consumed multiple times (duplicates)

#### 5.4.9. Practical Applications
These verification mechanisms are crucial for validating:
- At-least-once delivery : Ensures no messages are missed (no gaps in sequence) and messages are processed in sequence order
- Exactly-once processing : Ensures no duplicates are processed through embedded transactional processors

## 6. API Definitions

The framework exposes REST APIs for control and monitoring.

### 6.1. Coordinator API

*   `POST /proofs`: Create/start a new verification test.
    *   **Request Body:** (`application/json`) - Defines the parameters for the new test.
        ```json
        {
          "name": "kafka-ordering-test-1",
          "driver": "kafka-driver-config-name",
          "drivers": {
            "admin": "admin-driver-name",
            "producer": "producer-driver-name",
            "consumer": "consumer-driver-name"
          },
          "features": ["ordering", "at_least_once"],
          "description": "Verify ordering and at-least-once delivery for Kafka",
          "topic": "proof-topic-1",
          "partitions": 10,
          "producers": 4,
          "consumers": 4,
          "msgRate": 1000,
          "keys": 100,
          "checkPointInterval": 60,
          "timeout": 180,
          "finalWaitSeconds": 60,
          "consumeDelay": 0,
          "duration": 28800
        }
        ```
    *   `timeout` controls checkpoint verification while the proof is running.
    *   `finalWaitSeconds` controls the extra wait for consumers to catch up after producers stop.
*   `GET /proofs`: List active tests.
    *   **Response Body:** (`application/json`) - A list of active proofs.
        ```json
        [
          {
            "id": "proof-abc-123",
            "name": "kafka-ordering-test-1",
            "driver": "kafka-driver-config-name",
            "features": ["ordering", "at_least_once"],
            "topic": "proof-topic-1",
            "status": "RUNNING",
            "startTime": 1678886400000,
            "duration": 28800,
            "msgRate": 1000
          },
          {
            "id": "proof-xyz-789",
            "name": "pulsar-exactly-once-test",
            "driver": "pulsar-driver-config-name",
            "features": ["exactly-once"],
            "topic": "proof-topic-2",
            "status": "COMPLETED",
            "startTime": 1678880000000,
            "duration": 3600,
            "msgRate": 500
          }
        ]
        ```
*   `GET /proofs/{id}`: Get proof summary and results for a specific test.
    *   **Path Parameter:** `id` (string) - The unique ID of the proof test.
    *   **Response Body:** (`application/json`) - Summary of the proof with high-level statistics.
        ```json
        {
          "id": "proof-abc-123",
          "name": "kafka-ordering-test-1",
          "status": "RUNNING",
          "results": {
            "verified": 1500000,
            "errors": 0,
            "outOfOrders": 2,
            "missed": 0,
            "duplicates": 5,
            "writeDuplicates": 0,
            "timeouts": 0
          },
          "checkpoints": { 
             "latestProducer": {},
             "latestConsumer": {}
          }
        }
        ```
*   `GET /proofs/{id}/details`: Get comprehensive details about a specific test.
    *   **Path Parameter:** `id` (string) - The unique ID of the proof test.
    *   **Response Body:** (`application/json`) - Detailed information of proof test including comprehensive error details.
        ```json
        {
          "proof": {
            "id": "proof-abc-123",
            "name": "kafka-ordering-test-1",
            "driver": "kafka-driver-config-name",
            "features": ["ordering", "at_least_once"],
            "topic": "proof-topic-1",
            "partitions": 10,
            "producers": 4,
            "consumers": 4,
            "msgRate": 1000,
            "keys": 100,
            "checkPointInterval": 60,
            "timeout": 180,
            "finalWaitSeconds": 60,
            "duration": 28800,
            "startTime": 1678886400000
          },
          "summary": {
            "verified": 1500000,
            "errors": 0,
            "outOfOrders": 2,
            "missed": 0,
            "duplicates": 5,
            "writeDuplicates": 0,
            "timeouts": 0
          },
          "checkpoints": {
             "inCheck": {},
             "latestProducer": {},
             "latestConsumer": {},
             "verifiedProducer": {},
             "verifiedConsumer": {},
             "failedProducer": {},
             "failedConsumer": {}
          },
          "failedKeys": {
            "key1": [{ "seq": 42, "metadata": { "offset": 1024 } }]
          },
          "missedSeqs": {
            "key2": [{ "start": { "seq": 100, "metadata": { "offset": 2000 } }, 
                      "end": { "seq": 105, "metadata": { "offset": 2005 } }, 
                      "duplicated": 0 }]
          },
          "outOfOrderSeqs": {
            "key3": [[150, 145]]
          },
          "writeDuplicatesSeqs": {
            "key4": [{ "start": { "seq": 200, "metadata": { "offset": 3000 } }, 
                      "end": { "seq": 205, "metadata": { "offset": 3005 } }, 
                      "duplicated": 0 }]
          }
        }
        ```
*   `PUT /proofs/{id}/stop`: Stop a running test.
    *   **Path Parameter:** `id` (string) - The unique ID of the proof test.
    *   **Response Body:** (`application/json`) - Confirmation status.
        ```json
        {
          "status": "STOP_REQUESTED",
          "message": "Stop signal sent to proof test proof-abc-123"
        }
        ```
*   `DELETE /proofs/{id}`: Delete a test record.
    *   **Path Parameter:** `id` (string) - The unique ID of the proof test.
    *   **Response Body:** (`application/json`) - Confirmation status.
        ```json
        {
          "status": "DELETED",
          "message": "Proof test record proof-abc-123 deleted"
        }
        ```
*   `GET /configs`: Retrieve current worker and driver configurations.
    *   **Response Body:** (`application/json`) - The current configuration.
        ```json
        {
          "workers": {
            "worker-1": "http://10.0.1.10:8080",
            "worker-2": "http://10.0.1.11:8080"
          },
          "drivers": {
            "kafka-driver-config-name": {
              "type": "kafka",
              "configs": {
                "bootstrap.servers": "kafka-broker:9092",
                "security.protocol": "SASL_SSL",
              }
            },
            "pulsar-driver-config-name": {
              "type": "pulsar",
              "configs": {
                "serviceUrl": "pulsar://pulsar-broker:6650",
              }
            }
          }
        }
        ```
*   `PUT /configs`: Add/update configurations (replaces the entire configuration).
    *   **Request Body:** (`application/json`)
        ```json
        {
          "workers": {
            "worker-1": "http://10.0.1.10:8080",
            "worker-2": "http://10.0.1.11:8080",
            "worker-3": "http://10.0.1.12:8080"
          },
          "drivers": {
            "kafka-driver-config-name": {
              "type": "kafka",
              "configs": {
                "bootstrap.servers": "kafka-broker-new:9092",
                "security.protocol": "SASL_SSL"
              }
            }
          }
        }
        ```
*   `DELETE /configs`: Delete configurations (clears all configurations).
    *   **Response Body:** (`application/json`) - Confirmation status.
        ```json
        {
          "status": "CLEARED",
          "message": "All worker and driver configurations have been cleared."
        }
        ```
### 6.2. Worker API

*   `POST /producers/start`: Start producer instances on the worker.
    *   **Request Body:** (`application/json`) - Configuration for the producer task.
        ```json
        {
          "id": "proof-abc-123",
          "topic": "proof-topic-1",
          "producers": 4,
          "keys": 100,
          "msgRate": 1000,
          "driverName": "kafka",
          "driver": {
            "driverType": "kafka",
            "driverConfigs": {
              "bootstrap.servers": "kafka:9092",
              "client.id": "streaming-proof-producer"
            }
          }
        }
        ```
*   `POST /consumers/start`: Start consumer instances on the worker.
    *   **Request Body:** (`application/json`) - Configuration for the consumer task.
        ```json
        {
          "id": "proof-abc-123",
          "topic": "proof-topic-1",
          "partitions": 5,
          "consumers": 3,
          "consumeDelayMs": 0,
          "driverName": "kafka-driver-name",
          "driver": {
            "driverType": "kafka",
            "driverConfigs": {
              "driverType": "kafka",
              "driverConfigs": {
                "bootstrap.servers": "localhost:9092",
                "client.id": "streaming-proof-consumer"
              }
            }
          }
        }
        ```
*   `GET /producers/checkpoints/{id}`: Get the current producer checkpoint state.
    *   **Path Parameter:** `id` (string) - The `producerId` assigned during start.
    *   **Response Body:** (`application/json`)
        ```json
        {
           "published": {
               "key0": {
                   "seq": 1500,
                   "metadata": {
                       "offset": 1500,
                       "ledgerId": 123,
                       "entryId": 456,
                       "partition": 0
                   }
               },
               "key1": {
                   "seq": 1498,
                   "metadata": {
                       "offset": 1498,
                       "ledgerId": 123,
                       "entryId": 455,
                       "partition": 0
                   }
               },
           },
           "errors": {
               "key0": 0,
               "key1": 2,
           }
        }
        ```
*   `POST /consumers/checkpoints/{id}`: Get the current consumer checkpoint state, optionally applying watermarks.
    *   **Path Parameter:** `id` (string) - The `consumerId` assigned during start.
    *   **Request Body:** (`application/json`) - Optional map of high watermarks per key. Workers trim ranges at or below the watermark before returning the checkpoint. An empty map `{}` skips trimming.
        ```json
        {
          "key0": 1500,
          "key1": 1498
        }
        ```
    *   **Response Body:** (`application/json`) - The ConsumerCheckPoint object for this consumer instance.
        ```json
        {
           "consumed": {
              "key0": {
                 "2023-04-15T10:30:00": {
                    "start": {
                       "seq": 1000,
                       "metadata": { "offset": 1234 }
                    },
                    "end": {
                       "seq": 1499,
                       "metadata": { "offset": 1733 }
                    },
                    "duplicated": 0
                 }
              },
              "key1": {
              }
           },
           "missedSeqs": {
              "key0": [
                 {
                    "start": { "seq": 1500, "metadata": { "offset": 1733 } },
                    "end": { "seq": 1505, "metadata": { "offset": 1740 } },
                    "duplicated": 0
                 }
              ]
           },
           "duplicatedCount": {
              "key0": 3,
              "key1": 0
           },
           "outOfOrderSeqs": {
              "key0": [
                 [1200, 1198]
              ]
           }
        }
        ```

        The ConsumerCheckPoint tracks:
        - `consumed`: Map of sequence ranges for each key, with timestamps as inner keys
        - `missedSeqs`: Detected gaps in sequence numbers for each key
        - `duplicatedCount`: Count of duplicated messages per key
        - `outOfOrderSeqs`: Pairs of sequence numbers received out of order
*   `POST /producers/stop/{id}`: Stop producer instances.
    *   **Path Parameter:** `id` (string) - The `proofId`.

*   `POST /consumers/stop/{id}`: Stop consumer instances.
    *   **Path Parameter:** `id` (string) - The `proofId`.

## 7. Data Structure Design

The framework uses a two-tier data structure approach to provide both high-level summaries and detailed error information:

### 7.1. ProofSummary
The `ProofSummary` provides high-level statistics for quick monitoring and overview:
- `verified`: Total number of messages successfully verified
- `errors`: Count of errors encountered during message processing
- `outOfOrders`: Number of messages received out of their expected sequence
- `missed`: Count of messages that were expected but never received
- `duplicates`: Number of messages that were received multiple times
- `writeDuplicates`: Number of write-side duplicate messages
- `timeouts`: Count of timeout events during message processing

### 7.2. ProofDetails
The `ProofDetails` provides comprehensive information including:
- `proof`: The original test configuration
- `summary`: The ProofSummary object with high-level statistics
- `checkpoints`: Complete checkpoint information from all producers and consumers
- `failedKeys`: Detailed mapping of failed keys and their associated sequence numbers
- `missedSeqs`: Mapping of consumer identifiers to ranges of missed sequence numbers
- `outOfOrderSeqs`: Mapping of consumer identifiers to pairs of out-of-order sequence numbers
- `writeDuplicatesSeqs`: Mapping of producer identifiers to ranges of duplicate sequence numbers

### 7.3. Design Rationale
This separation provides several benefits:
- **Performance**: The summary endpoint (`GET /proofs/{id}`) returns quickly with essential statistics
- **Scalability**: Detailed error information is only computed when explicitly requested
- **Clarity**: Clear distinction between high-level monitoring and detailed debugging information
- **Flexibility**: Different use cases can access appropriate levels of detail

### 7.4. Usage Guidelines
- Use `GET /proofs/{id}` for real-time monitoring and dashboard displays
- Use `GET /proofs/{id}/details` for detailed analysis, debugging, and comprehensive reporting
- The detailed endpoint is particularly useful for investigating specific error patterns and sequence violations

## 8. Exactly-Once Semantics Verification

The streaming-proof framework supports verification of Kafka's exactly-once transaction semantics through embedded transactional processors. This feature ensures that messages are processed exactly once, with no duplicates or data loss.

### 8.1. How It Works

When the `exactly_once` feature is enabled, the framework automatically:

1. **Producers write to input topic**: `{topic}_transactional`
2. **Embedded transactional processor**: Reads from input topic and writes to output topic atomically
3. **Consumers read from output topic**: `{topic}` for verification
4. **Verification**: Standard `ProofConsumerTask` verifies exactly-once semantics

```
Producer → {topic}_transactional → TransactionalProcessor → {topic} → Consumer
```

### 8.2. Usage

#### 8.2.1. Enable Exactly-Once in Proof Configuration

```json
{
  "name": "kafka-exactly-once-test",
  "driver": "kafka",
  "features": ["exactly_once"],
  "topic": "test-topic",
  "partitions": 10,
  "producers": 4,
  "consumers": 4,
  "msgRate": 1000
}
```

#### 8.2.2. Coordinator API Test

```java
@Test
public void testExactlyOnceVerification() throws Exception {
    Proof proof = Proof.builder()
        .name("exactly-once-test")
        .driver("kafka_driver")
        .features(List.of("exactly_once"))  // Enable exactly-once
        .keys(100)
        .partitions(10)
        .producers(4)
        .consumers(4)
        .build();
    
    // Framework automatically handles transactional processing
    httpClient.createProof(proof).join();
}
```

### 8.3. What Gets Verified

The exactly-once verification tests:

- **Atomicity**: Consumer offsets and producer messages are committed together
- **No duplicates**: Each message appears exactly once in the output topic
- **No data loss**: All input messages appear in the output topic
- **Partition ordering**: Messages maintain their partition ordering
- **Transaction isolation**: Only committed transactions are visible to consumers

### 8.4. Implementation Details

- **Automatic setup**: No manual configuration of transactional processors needed
- **Embedded processors**: One transactional processor per producer instance
- **Same partition processing**: Maintains message ordering within partitions
- **Standard verification**: Existing consumer verification logic unchanged
- **Kafka-specific**: Only works with Kafka driver

### 8.5. Architecture Components for Exactly-Once

#### 8.5.1. KafkaExactlyOnceProofProducer
- Embeds a `KafkaTransactionalProcessor` instance
- Sends messages to the input topic (`{topic}_transactional`)
- Automatically starts the embedded processor for read-process-write operations

#### 8.5.2. KafkaTransactionalProcessor
- Implements the read-process-write pattern for exactly-once semantics
- Consumes from input topic and produces to output topic atomically
- Commits consumer offsets and producer messages in the same transaction
- Ensures proper transaction lifecycle (start before consume, commit after write)

#### 8.5.3. Topic Flow
- **Input Topic**: `{topic}_transactional` - where producers send messages
- **Output Topic**: `{topic}` - where consumers read verified messages
- **Automatic Creation**: Both topics are created automatically when exactly-once is enabled

### 8.6. Benefits

- **Simple configuration**: Just add `exactly_once` to features list
- **Framework integration**: Works seamlessly with existing proof tests
- **True exactly-once testing**: Tests the complete read-process-write pattern
- **Production-like**: Uses the same patterns as real exactly-once applications
- **Type-safe architecture**: Eliminates fragile reflection-based instantiation
