# Streaming Proof

Streaming Proof is a robust testing framework designed to verify ordering and delivery guarantees in distributed streaming systems like Apache Kafka, Apache Pulsar, and other messaging platforms. It offers a streamlined approach to validate the correctness of streaming systems through automated testing and monitoring. With Streaming Proof, you can easily verify that your streaming system meets its promised delivery guarantees. For a deeper understanding of the framework, please check the [design document](doc/The_framework_for_streaming_correctness_verification.md).

**Key Benefits**:
- Simple yet powerful correctness verification at scale
- Comprehensive test management through REST APIs
- Minimal external dependencies
- Flexible deployment options (Docker, Kubernetes)
- Zone-aware deployment capabilities

Here is the example to show the proof status:

```json
{
  "proof" : {
    "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
    "name" : "Kafka: at_least_once + ordering",
    "driver" : "kafka",
    "features" : [ "at_least_once", "ordering" ],
    "description" : null,
    "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
    "partitions" : 10,
    "producers" : 4,
    "consumers" : 4,
    "msgRate" : 1000,
    "keys" : 40,
    "checkPointInterval" : 5,
    "timeout" : 180,
    "maxStallSeconds" : 300,
    "duration" : 28800,
    "startTime" : 1738794638049
  },
  "summary" : {
    "verified" : 1765040,
    "errors" : 0,
    "outOfOrders" : 0,
    "missed" : 0,
    "duplicates" : 716,
    "timeouts" : 0,
    "verifiedStallSeconds" : 4,
    "maxVerifiedStallSeconds" : 12
  },
  "checkpoints" : {...}
}
```

- **verified**: Total number of messages successfully verified
- **errors**: Number of failures during message publication
- **outOfOrders**: Number of messages received out of sequence
- **missed**: Number of messages not received for verification
- **duplicates**: Number of duplicate messages detected
- **timeouts**: Number of verification attempts that exceeded the configured timeout period
- **verifiedStallSeconds**: Wall-clock seconds since the verified count last increased; accumulates from test start and freezes when the run completes
- **maxVerifiedStallSeconds**: Peak verification stall observed during the run, in seconds; retained across recoveries and frozen at completion

The checkpoints section provides detailed information about the verification status of each message key, allowing you to track the progress and health of your streaming system at a granular level.

**Detecting stalled verification**

`verifiedStallSeconds` and `maxVerifiedStallSeconds` are always reported, so you can watch verification progress even without any extra configuration:

- `verifiedStallSeconds` is the *current* stall — it climbs while `verified` sits still and resets to `0` as soon as `verified` moves again.
- `maxVerifiedStallSeconds` is the *worst* stall seen during the whole run. Because it survives recoveries, it catches a run that froze for a long time mid-way and then caught up — something the current value alone would hide.

To make a long stall actually fail the run, set `maxStallSeconds` on the proof when you create it. It is the maximum acceptable peak stall in seconds; if `maxVerifiedStallSeconds` exceeds it, the result is marked `failed` with a reason like `Verification stalled for 340s, exceeding the configured limit of 300s.`

```json
{
  "name": "kafka-stall-guarded",
  "driver": "kafka",
  "features": ["at_least_once", "ordering"],
  "topic": "orders",
  "maxStallSeconds": 300
}
```

The check is **opt-in**: `maxStallSeconds` defaults to `0`, which disables it entirely. With no value set (or `0`), the stall metrics are still shown but never affect the pass/fail result.

**Supported Streaming Systems**:
- Apache Kafka, or any Kafka API compatible systems
- Apache Pulsar (Failover, Key_Shared, Shared subscriptions, and Pulsar 5.0 scalable topics)

**Supported verifications**:
- At least once
- Ordering
- Shared subscription (Pulsar) — at-least-once delivery with duplicate counting across round-robin consumers

## Build
### Building from Source

- Java 21 or later
- Maven 3.6 or later

```bash
mvn clean install -DskipTests
```

### Building Docker Image

- Java 21 or later
- Maven 3.6 or later

```bash
mvn clean install -DskipTests -Pdocker
```
## Release

Automated Docker image publishing is not currently configured. Build the image locally with the
Docker Maven profile shown above.

## Quick Start with Docker Compose

Follow these simple steps to get Streaming Proof up and running quickly with Docker Compose:

1. Start the services with docker-compose:

```bash
cd deploy/docker-compose/kafka
docker-compose up -d
```
This will start:

- Proof Coordinator service (port 8080)
- Proof Worker service (ports 8088)
- Kafka (port 9092)
- ZooKeeper (port 2181)

2. Verify the deployment:

```bash
docker-compose ps
```
3. Init configurations:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers" : {
    "worker.1" : "http://worker:8088"
  },
  "drivers" : {
    "kafka" : {
      "driverType" : "kafka",
      "driverConfigs" : {
        "acks" : "1",
        "bootstrap.servers" : "kafka:9092",
        "session.timeout.ms" : "45000"
      }
    }
  }
}'
```
4. Create first proof:

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name" : "Kafka: at_least_once + ordering",
  "driver" : "kafka",
  "features" : [ "at_least_once", "ordering" ],
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180,
  "finalWaitSeconds" : 60,
  "kafka" : {
    "topicConfig" : {
      "unclean.leader.election.enable" : "true"
    }
  }
}'
```

`timeout` controls checkpoint verification while the proof is running. `finalWaitSeconds` controls the extra wait for consumers to catch up after producers stop.

`messageSize` sets the total byte size of each message value and defaults to 8, the size of the
sequence number alone. Larger values pad each message so that storage-path behaviour (offload
triggers, tiered-storage reads, bandwidth) can be exercised at a realistic scale without pushing
`msgRate` unrealistically high. The padding is filler and is never verified; values below 8 are
rejected. It applies to the Kafka, Pulsar, and MQTT drivers alike.

Kafka topic configuration is passed through to `NewTopic.configs()` without vendor-specific handling.
For example,
`unclean.leader.election.enable` can be set to `"true"` when explicitly testing that Kafka
behavior. The same configuration is applied to the transactional topic used by exactly-once
proofs.

### Pulsar Shared Subscription Verification

To verify at-least-once delivery with Pulsar Shared subscriptions, configure the `pulsar` field with `subscriptionType: "Shared"`:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers": {
    "worker.1": "http://worker:8088"
  },
  "drivers": {
    "pulsar_driver": {
      "driverType": "pulsar",
      "driverConfigs": {
        "pulsar.service.url": "pulsar://localhost:6650",
        "pulsar.admin.url": "http://localhost:8080"
      }
    }
  }
}'
```

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name": "Pulsar Shared subscription test",
  "driver": "pulsar_driver",
  "features": ["at_least_once"],
  "partitions": 1,
  "producers": 2,
  "consumers": 4,
  "msgRate": 500,
  "keys": 10,
  "checkPointInterval": 5,
  "timeout": 180,
  "finalWaitSeconds": 60,
  "pulsar": {
    "consumerConfig": {
      "subscriptionType": "Shared"
    }
  }
}'
```

With Shared subscriptions, messages are round-robin distributed across consumers — per-key ordering is not guaranteed. The framework uses **high-watermark-based verification** instead of last-sequence comparison: it computes the highest contiguous sequence per key across all consumers, and verifies that every produced message has been consumed. Out-of-order delivery is expected and not flagged.

### Pulsar Scalable Topics

To verify [scalable topics](https://github.com/apache/pulsar/tree/master/spec/scalable-topics) on an Apache Pulsar
5.0+ cluster, enable them in the proof's `pulsar` section. Any Pulsar driver works:

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name": "Pulsar scalable topic",
  "driver": "pulsar_driver",
  "features": ["at_least_once", "ordering"],
  "producers": 2,
  "consumers": 4,
  "msgRate": 1000,
  "keys": 100,
  "pulsar": {
    "scalableTopicEnabled": true,
    "initialSegments": 4,
    "consumerType": "stream"
  }
}'
```

The proof creates `topic://public/default/<topic>` and produces and consumes with the Pulsar V5 client. Scalable
topics have no partitions or subscription types, so `partitions` and `pulsar.consumerConfig` are rejected:

- `initialSegments` replaces `partitions`, 10 when not set.
- `consumerType` picks one of the Pulsar 5 consumers:
  - `stream` (default): per-key ordered consumption, verified with `at_least_once` and `ordering` across segment
    splits and merges.
  - `queue`: unordered consumption, verified like Shared subscriptions.
  - `checkpoint`: per-key ordered reads with no broker-managed position or acknowledgment, verified like `stream`.
    The consumers form a group that shares the segments, and each segment is read from its first message.

The `offload.condition.*` settings apply to classic topics only. The broker splits and merges segments on its own
unless auto scaling is disabled for the namespace or topic.

5. Stop all the components:

```bash
docker-compose down
```

## Deployment

## Deploy with helm chart

```bash
cd streaming-proof/deploy/helm
helm install streaming-proof ./streaming-proof -n streaming-proof
```

The chart does not include worker or messaging-cluster connection details. Configure them through
the `PUT /configs` API, or provide a private values file containing a `configs` map. Do not commit
credentials to the chart values.

### Minimal UI Prototype

The coordinator now includes a minimal dynamic UI prototype served from the same port as the API.

After starting the coordinator, open:

```bash
http://localhost:8080/ui/index.html
```

The page reads from the live coordinator APIs:

- `GET /proofs`
- `GET /proofs/{id}/report`

This is intentionally a small prototype to validate the browsing flow before adding
performance latencies, persistence, and richer reporting.

The current report view already includes a lightweight performance summary derived
from the live checkpoints, such as elapsed time, publish and consume rates, and
overall progress through the planned duration.

## Create proof

1. Create workers and drivers configuration:

```bash
curl -X PUT http://localhost:8080/configs \
  -H "Content-Type: application/json" \
  -d '{
  "workers" : {
    "worker.1" : "http://worker.1:8088",
    "worker.2" : "http://worker.2:8088",
    "worker.3" : "http://worker.3:8088"
  },
  "drivers" : {
    "kafka" : {
      "driverType" : "kafka",
      "driverConfigs" : {
        "security.protocol" : "SASL_PLAINTEXT",
        "acks" : "1",
        "sasl.mechanism" : "PLAIN",
        "sasl.jaas.config" : "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='token:<your-token>';",
        "client.id" : "zone_id={zone.id}",
        "bootstrap.servers" : "kafka.0:9092,kafka.1:9092,kafka.12:9092",
        "session.timeout.ms" : "45000"
      }
    }
  }
}'
```

2. Create and start a proof:

```bash
curl -X POST http://localhost:8080/proofs \
  -H "Content-Type: application/json" \
  -d '{
  "name" : "Kafka: at_least_once + ordering",
  "driver" : "kafka",
  "features" : [ "at_least_once", "ordering" ],
  "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180,
  "finalWaitSeconds" : 60
}'
```

3. Get the list of proofs:

```bash
curl http://localhost:8080/proofs
```

output:

```json
[ {
  "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
  "name" : "Kafka: at_least_once + ordering",
  "driver" : "kafka",
  "features" : [ "at_least_once", "ordering" ],
  "description" : null,
  "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
  "partitions" : 10,
  "producers" : 4,
  "consumers" : 4,
  "msgRate" : 1000,
  "keys" : 40,
  "checkPointInterval" : 5,
  "timeout" : 180,
  "finalWaitSeconds" : 60,
  "duration" : 28800,
  "startTime" : 1738794638049
} ]
```

4. Get the proof status:

```bash
curl http://localhost:8080/proofs/137ba4d8-50ad-4c96-8c16-101b54083771
```

output:

```json
{
  "proof" : {
    "id" : "137ba4d8-50ad-4c96-8c16-101b54083771",
    "name" : "Kafka: at_least_once + ordering",
    "driver" : "kafka",
    "features" : [ "at_least_once", "ordering" ],
    "description" : null,
    "topic" : "9e673a76-77b2-4d20-b805-c59f49062d3a",
    "partitions" : 10,
    "producers" : 4,
    "consumers" : 4,
    "msgRate" : 1000,
    "keys" : 40,
    "checkPointInterval" : 5,
    "timeout" : 180,
    "finalWaitSeconds" : 60,
    "duration" : 28800,
    "startTime" : 1738794638049
  },
  "summary" : {
    "verified" : 1765040,
    "errors" : 0,
    "outOfOrders" : 0,
    "missed" : 0,
    "duplicates" : 716,
    "timeouts" : 0
  },
  "checkpoints" : {...}
}
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
