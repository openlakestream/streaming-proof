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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.ProofValue;
import io.lakestream.streaming.proof.common.Util;
import java.util.Map;
import org.testng.annotations.Test;

public class PulsarProofConfigTest {

    @Test
    public void testCheckpointRecoveryProofConfigurationRoundTrip() throws Exception {
        PulsarProofConfig config = PulsarProofConfig.builder().scalableTopicEnabled(true)
                .consumerType("checkpoint").checkpointRestartMessages(10).build();
        PulsarProofConfig restored = Util.JSON_MAPPER.readValue(
                Util.JSON_MAPPER.writeValueAsString(config), PulsarProofConfig.class);
        assertEquals(restored.getCheckpointRestartMessages(), Integer.valueOf(10));
        assertTrue(restored.isCheckpointRecoveryEnabled());
        assertNull(Util.JSON_MAPPER.readValue("{}", PulsarProofConfig.class).getCheckpointRestartMessages());
    }

    @Test
    public void testSerializationRoundTrip() throws Exception {
        PulsarProofConfig config = PulsarProofConfig.builder()
                .consumerConfig(Map.of("subscriptionType", "Key_Shared"))
                .build();

        String json = Util.JSON_MAPPER.writeValueAsString(config);
        PulsarProofConfig deserialized = Util.JSON_MAPPER.readValue(json, PulsarProofConfig.class);

        assertNotNull(deserialized.getConsumerConfig());
        assertEquals(deserialized.getConsumerConfig().get("subscriptionType"), "Key_Shared");
    }

    @Test
    public void testDeserializationWithNullConsumerConfig() throws Exception {
        String json = "{}";
        PulsarProofConfig deserialized = Util.JSON_MAPPER.readValue(json, PulsarProofConfig.class);
        assertNull(deserialized.getConsumerConfig());
    }

    @Test
    public void testProofWithPulsarConfig() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"],
                  "pulsar": {
                    "consumerConfig": {
                      "subscriptionType": "Key_Shared",
                      "receiverQueueSize": 500
                    }
                  }
                }
                """;
        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);
        assertNotNull(proof.getPulsar());
        assertEquals(proof.getPulsar().getConsumerConfig().get("subscriptionType"), "Key_Shared");
        assertEquals(proof.getPulsar().getConsumerConfig().get("receiverQueueSize"), 500);
    }

    @Test
    public void testProofWithoutPulsarConfig() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"]
                }
                """;
        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);
        assertNull(proof.getPulsar());
    }

    @Test
    public void testNewConsumersSerializationWithPulsarConfig() throws Exception {
        Map<String, Object> pulsarConfig = Map.of("subscriptionType", "Key_Shared");
        NewConsumers consumers = new NewConsumers(
                "proof-1", "topic-1", 4, 2, 0L, ProofValue.MIN_SIZE, "pulsar",
                new Driver("pulsar", Map.of("pulsar.service.url", "pulsar://localhost:6650")),
                pulsarConfig, null);

        String json = Util.JSON_MAPPER.writeValueAsString(consumers);
        NewConsumers deserialized = Util.JSON_MAPPER.readValue(json, NewConsumers.class);

        assertNotNull(deserialized.pulsarConsumerConfig());
        assertEquals(deserialized.pulsarConsumerConfig().get("subscriptionType"), "Key_Shared");
    }

    @Test
    public void testNewConsumersSerializationWithoutPulsarConfig() throws Exception {
        NewConsumers consumers = new NewConsumers(
                "proof-1", "topic-1", 4, 2, 0L, ProofValue.MIN_SIZE, "pulsar",
                new Driver("pulsar", Map.of("pulsar.service.url", "pulsar://localhost:6650")),
                null, null);

        String json = Util.JSON_MAPPER.writeValueAsString(consumers);
        NewConsumers deserialized = Util.JSON_MAPPER.readValue(json, NewConsumers.class);

        assertNull(deserialized.pulsarConsumerConfig());
        assertNull(deserialized.pulsarConsumerType());
    }

    @Test
    public void testProofWithScalableTopic() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"],
                  "pulsar": {
                    "scalableTopicEnabled": true,
                    "initialSegments": 4,
                    "consumerType": "queue"
                  }
                }
                """;
        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);
        assertTrue(proof.isScalableTopic());
        assertNull(proof.getPartitions());
        assertEquals(proof.getPulsar().getInitialSegments(), Integer.valueOf(4));
        assertEquals(proof.getPulsar().getConsumerType(), "queue");
    }

    @Test
    public void testSharedMode() {
        assertTrue(PulsarProofConfig.builder().consumerType("queue").build().isSharedMode());
        assertTrue(PulsarProofConfig.builder()
                .consumerConfig(Map.of("subscriptionType", "Shared")).build().isSharedMode());
        assertFalse(PulsarProofConfig.builder().consumerType("stream").build().isSharedMode());
        assertFalse(PulsarProofConfig.builder().consumerType("checkpoint").build().isSharedMode());
        assertFalse(new PulsarProofConfig().isSharedMode());
    }

    @Test
    public void testNewConsumersSerializationWithConsumerType() throws Exception {
        NewConsumers consumers = new NewConsumers(
                "proof-1", "topic-1", 4, 2, 0L, ProofValue.MIN_SIZE, "pulsar",
                new Driver("pulsar", Map.of("pulsar.service.url", "pulsar://localhost:6650")),
                null, "queue");

        String json = Util.JSON_MAPPER.writeValueAsString(consumers);
        NewConsumers deserialized = Util.JSON_MAPPER.readValue(json, NewConsumers.class);

        assertEquals(deserialized.pulsarConsumerType(), "queue");
    }

    @Test
    public void testDriverMetadataDeserialization() throws Exception {
        String json = """
                {
                  "workers": {
                    "worker.1": "http://worker:8088"
                  },
                  "drivers": {
                    "local_pulsar": {
                      "driverType": "pulsar",
                      "driverConfigs": {
                        "pulsar.service.url": "pulsar://cluster:6650",
                        "pulsar.admin.url": "http://cluster:8080"
                      },
                      "metadata": {
                        "displayName": "Classic Stage",
                        "brokers": "3 x n2-standard-8",
                        "notes": "catchup validation cluster"
                      }
                    }
                  }
                }
                """;

        Configs configs = Util.JSON_MAPPER.readValue(json, Configs.class);
        Driver driver = configs.drivers().get("local_pulsar");

        assertNotNull(driver);
        assertNotNull(driver.metadata());
        assertEquals(driver.metadata().get("displayName"), "Classic Stage");
        assertEquals(driver.metadata().get("brokers"), "3 x n2-standard-8");
    }
}
