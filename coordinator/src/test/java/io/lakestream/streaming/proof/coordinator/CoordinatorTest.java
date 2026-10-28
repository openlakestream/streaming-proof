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
package io.lakestream.streaming.proof.coordinator;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CoordinatorTest {

    @Test
    public void testListProofsSortsByStartTimeDescending() {
        Coordinator coordinator = new Coordinator();
        addProof(coordinator, "oldest", "2026-03-31T10:00:00");
        addProof(coordinator, "newest", "2026-03-31T12:00:00");
        addProof(coordinator, "middle", "2026-03-31T11:00:00");

        List<Proof> proofs = coordinator.listProofs();

        assertEquals(
                proofs.stream().map(Proof::getId).toList(),
                List.of("newest", "middle", "oldest"));
    }

    @Test
    public void testListProofsWithStatusSortsByStartTimeDescending() {
        Coordinator coordinator = new Coordinator();
        addProof(coordinator, "without-time", null);
        addProof(coordinator, "newest", "2026-03-31T12:00:00");
        addProof(coordinator, "oldest", "2026-03-31T10:00:00");

        List<Map<String, Object>> proofs = coordinator.listProofsWithStatus();

        assertEquals(
                proofs.stream().map(item -> (String) item.get("id")).toList(),
                List.of("newest", "oldest", "without-time"));
        assertEquals(proofs.getFirst().get("status"), "running");
    }

    @Test
    public void testCreateProofRejectsNegativeFinalWaitSeconds() {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(
                Map.of(),
                Map.of("test-driver", new Driver("kafka", Map.of(), null))));

        Proof proof = Proof.builder()
                .name("invalid-final-wait")
                .driver("test-driver")
                .features(List.of("at_least_once"))
                .topic("test-topic")
                .finalWaitSeconds(-1)
                .build();

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
                () -> coordinator.createProof(proof));

        assertEquals(error.getMessage(), "Final wait seconds must be 0 or greater.");
    }

    @DataProvider
    public Object[][] nonPositiveProducerTotals() {
        return new Object[][] {
                {0, 16, 100, "Producers must be at least 1."},
                {-1, 16, 100, "Producers must be at least 1."},
                {3, 0, 100, "Keys must be at least 1."},
                {3, -1, 100, "Keys must be at least 1."},
                {3, 16, 0, "Message rate must be at least 1."},
                {3, 16, -1, "Message rate must be at least 1."}
        };
    }

    @Test(dataProvider = "nonPositiveProducerTotals")
    public void testCreateProofRejectsNonPositiveProducerTotals(int producers, int keys, int msgRate, String message) {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(
                Map.of(),
                Map.of("test-driver", new Driver("kafka", Map.of(), null))));
        Proof proof = Proof.builder()
                .name("invalid-producer-totals")
                .driver("test-driver")
                .features(List.of("at_least_once"))
                .topic("test-topic")
                .producers(producers).keys(keys).msgRate(msgRate)
                .build();

        assertCreateProofRejected(coordinator, proof, message);
        assertNull(proof.getId());
        assertEquals(coordinator.listProofs(), List.of());
    }

    @Test
    public void testCreateProofValidatesScalableTopicSettings() {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(
                Map.of(),
                Map.of("pulsar", new Driver("pulsar", Map.of(), null))));

        assertCreateProofRejected(coordinator, pulsarProof(PulsarProofConfig.builder()
                        .scalableTopicEnabled(true)
                        .build()).partitions(4).build(),
                "Scalable topics have no partitions, use pulsar.initialSegments instead.");
        assertCreateProofRejected(coordinator, pulsarProof(PulsarProofConfig.builder()
                        .scalableTopicEnabled(true)
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build()).build(),
                "Scalable topics have no subscription types, use pulsar.consumerType (stream, queue or "
                        + "checkpoint) instead of pulsar.consumerConfig.");
        assertCreateProofRejected(coordinator, pulsarProof(PulsarProofConfig.builder()
                        .scalableTopicEnabled(true)
                        .consumerType("Queue")
                        .build()).build(),
                "pulsar.consumerType must be stream, queue or checkpoint.");
        assertCreateProofRejected(coordinator, pulsarProof(PulsarProofConfig.builder()
                        .consumerType("queue")
                        .build()).build(),
                "pulsar.initialSegments and pulsar.consumerType require pulsar.scalableTopicEnabled.");
    }

    @DataProvider
    public Object[][] invalidCheckpointRecovery() {
        return new Object[][] {
                {false, "checkpoint", 1, 10},
                {true, "stream", 1, 10},
                {true, "queue", 1, 10},
                {true, "checkpoint", 2, 10},
                {true, "checkpoint", 0, 10},
                {true, "checkpoint", 1, 0},
                {true, "checkpoint", 1, -1}
        };
    }

    @Test(dataProvider = "invalidCheckpointRecovery")
    public void rejectsCheckpointRecoveryThatCanMaskLoss(boolean scalable, String type, int consumers, int restart) {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar", Map.of()))));
        Proof proof = pulsarProof(PulsarProofConfig.builder().scalableTopicEnabled(scalable)
                .consumerType(type).checkpointRestartMessages(restart).build()).consumers(consumers).build();
        expectThrows(IllegalArgumentException.class, () -> coordinator.createProof(proof));
        assertNull(proof.getId());
        assertEquals(coordinator.listProofs(), List.of());
    }

    @DataProvider
    public Object[][] nonScalableRecoveryUris() {
        return new Object[][] {
                {"persistent://public/default/recovery"},
                {"non-persistent://public/default/recovery"},
                {"other://public/default/recovery"},
                {"TOPIC://public/default/recovery"}
        };
    }

    @Test(dataProvider = "nonScalableRecoveryUris")
    public void rejectsExplicitNonScalableRecoveryUrisBeforeCreatingResources(String topic) {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar", Map.of()))));
        Proof proof = pulsarProof(PulsarProofConfig.builder().scalableTopicEnabled(true)
                .consumerType("checkpoint").checkpointRestartMessages(20).build())
                .topic(topic).consumers(1).build();
        assertCreateProofRejected(coordinator, proof,
                "Checkpoint recovery requires a topic:// URI or an unqualified topic name.");
        assertNull(proof.getId());
        assertEquals(coordinator.listProofs(), List.of());
    }

    @Test
    public void rejectsDriverWideCheckpointRecovery() {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar",
                Map.of("pulsar.checkpoint.restart.messages", 10)))));
        Proof proof = pulsarProof(PulsarProofConfig.builder().scalableTopicEnabled(true)
                .consumerType("checkpoint").build()).consumers(1).build();
        assertCreateProofRejected(coordinator, proof,
                "Use per-proof pulsar.checkpointRestartMessages, not the driver-wide restart setting.");
        assertNull(proof.getId());
    }

    private static Proof.ProofBuilder pulsarProof(PulsarProofConfig pulsar) {
        return Proof.builder()
                .name("scalable")
                .driver("pulsar")
                .features(List.of("at_least_once"))
                .topic("test-topic")
                .pulsar(pulsar);
    }

    private static void assertCreateProofRejected(Coordinator coordinator, Proof proof, String message) {
        assertEquals(expectThrows(IllegalArgumentException.class, () -> coordinator.createProof(proof))
                .getMessage(), message);
    }

    private static void addProof(Coordinator coordinator, String id, String startTime) {
        Proof proof = Proof.builder()
                .id(id)
                .name(id)
                .startTime(startTime)
                .build();
        ProofTask task = mock(ProofTask.class);
        doReturn(proof).when(task).getProof();
        doReturn(new ProofReport(proof, "running", null, null, null, null, null, null, null))
                .when(task).getReport();
        coordinator.getProofs().put(id, task);
    }
}
