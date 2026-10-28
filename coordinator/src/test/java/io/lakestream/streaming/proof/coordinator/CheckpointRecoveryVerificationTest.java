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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.NewConsumers;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import io.lakestream.streaming.proof.worker.ProofConsumers;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CheckpointRecoveryVerificationTest {
    @DataProvider
    public Object[][] validRecoveryTopics() {
        return new Object[][] {
                {"recovery", "topic://public/default/recovery"},
                {"topic://public/default/recovery", "topic://public/default/recovery"}
        };
    }

    @Test(dataProvider = "validRecoveryTopics")
    public void acceptedRecoveryTopicsReachScalableAdminAndConsumerPaths(String topic, String expected)
            throws Exception {
        Proof proof = Proof.builder().id("routing").topic(topic).driver("pulsar").consumers(1)
                .features(List.of("at_least_once"))
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).initialSegments(1)
                        .consumerType("checkpoint").checkpointRestartMessages(20).build()).build();
        Method validate = Coordinator.class.getDeclaredMethod("validateScalableTopicSettings", Proof.class);
        validate.setAccessible(true);
        validate.invoke(null, proof);
        ProofDriver driver = mock(ProofDriver.class);
        ProofTask task = new ProofTask(proof,
                new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar", Map.of()))), driver);
        try {
            task.createProofTopics();
            verify(driver).createTopic(expected, 1, null);
            WorkerHttpClient worker = mock(WorkerHttpClient.class);
            when(worker.startConsumers(any())).thenReturn(CompletableFuture.completedFuture(null));
            task.getClients().add(worker);
            Method start = ProofTask.class.getDeclaredMethod("startConsumers");
            start.setAccessible(true);
            start.invoke(task);
            ArgumentCaptor<NewConsumers> request = ArgumentCaptor.forClass(NewConsumers.class);
            verify(worker).startConsumers(request.capture());
            assertEquals(request.getValue().topic(), expected);
            assertEquals(request.getValue().pulsarCheckpointRestartMessages(), Integer.valueOf(20));
            Method routing = io.lakestream.streaming.proof.driver.pulsar.PulsarProofDriver.class
                    .getDeclaredMethod("isScalableTopic", String.class);
            routing.setAccessible(true);
            assertEquals(routing.invoke(null, request.getValue().topic()), true);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void coordinatorForwardsRecoveryIntervalWithoutIncreasingTotalReaders() throws Exception {
        Proof proof = Proof.builder().id("forward").topic("topic://public/default/forward").driver("pulsar")
                .consumers(1).features(List.of("at_least_once"))
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).consumerType("checkpoint")
                        .checkpointRestartMessages(20).build()).build();
        ProofTask task = new ProofTask(proof,
                new Configs(Map.of(), Map.of("pulsar", new Driver("pulsar", Map.of()))), mock(ProofDriver.class));
        List<NewConsumers> assignments = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                WorkerHttpClient worker = mock(WorkerHttpClient.class);
                when(worker.startConsumers(any())).thenAnswer(invocation -> {
                    assignments.add(invocation.getArgument(0));
                    return CompletableFuture.completedFuture(null);
                });
                task.getClients().add(worker);
            }
            Method start = ProofTask.class.getDeclaredMethod("startConsumers");
            start.setAccessible(true);
            start.invoke(task);
            assertEquals(assignments.stream().mapToInt(NewConsumers::consumers).sum(), 1);
            for (NewConsumers assignment : assignments) {
                assertEquals(assignment.pulsarCheckpointRestartMessages(), Integer.valueOf(20));
                assertEquals(assignment.pulsarConsumerType(), "checkpoint");
            }
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void replayBelowTrimmedWatermarkSurvivesWorkerSerializationAndFailsFinalReport() throws Exception {
        NewConsumers request = new NewConsumers("recovery", "topic://public/default/recovery", 1, 1,
                0, 8, "pulsar", new Driver("pulsar", Map.of()), null, "checkpoint", 2);
        request = Util.JSON_MAPPER.readValue(Util.JSON_WRITER.writeValueAsString(request), NewConsumers.class);
        ProofDriver driver = mock(ProofDriver.class);
        ProofConsumer consumer = mock(ProofConsumer.class);
        when(consumer.name()).thenReturn("reader");
        when(driver.createConsumer(anyString(), anyInt(), anyLong(), any(), any())).thenReturn(consumer);
        ProofConsumers workers = new ProofConsumers(request, driver);
        workers.start();
        ArgumentCaptor<MessageListener> listener = ArgumentCaptor.forClass(MessageListener.class);
        ArgumentCaptor<Map<String, Object>> config = ArgumentCaptor.forClass(Map.class);
        verify(driver).createConsumer(anyString(), anyInt(), anyLong(), config.capture(), listener.capture());
        assertEquals(config.getValue().get("pulsar.checkpoint.restart.messages"), 2);
        listener.getValue().onMessage("key", 0, position(0, 0));
        listener.getValue().onMessage("key", 1, position(1, 0));
        workers.applyHighWatermarks(Map.of("key", 1L));
        // A sealed parent replay after a restore must fail even after its old ranges were discarded.
        listener.getValue().onMessage("key", 0, position(0, 0));
        listener.getValue().onMessage("key", 1, position(1, 0));
        listener.getValue().onMessage("key", 2, position(1, 1));
        workers.applyHighWatermarks(Map.of("key", 2L));
        ConsumerCheckPoint checkpoint = Util.JSON_MAPPER.readValue(
                Util.JSON_WRITER.writeValueAsString(workers.checkPoint()), ConsumerCheckPoint.class);
        ConsumerCheckPoint aggregate = new ConsumerCheckPoint();
        aggregate.merge(checkpoint);
        assertEquals(aggregate.getCheckpointReplayCount(), 2L);
        assertTrue(aggregate.getConsumed().isEmpty());
        assertEquals(workers.checkPointDetails().get("reader").getCheckpointReplayCount(), 2L);
        Proof proof = Proof.builder().topic(request.topic()).consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).consumerType("checkpoint")
                        .checkpointRestartMessages(2).build()).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            Field latest = ProofTask.class.getDeclaredField("latestConsumerCheckpoint");
            latest.setAccessible(true);
            latest.set(task, aggregate);
            assertEquals(task.getReport().summary().duplicates(), 2L);
            assertEquals(task.getReport().status(), "failed");
            assertTrue(task.getReport().resultReason().contains("replayed"));
            // Completion notifications must retain strict replay failures while cleanup is still running.
            task.getRunning().set(true);
            assertEquals(task.getReport().status(), "running");
            Method buildReport = ProofTask.class.getDeclaredMethod("buildReport", boolean.class);
            buildReport.setAccessible(true);
            ProofReport finalReport =
                    (ProofReport) buildReport.invoke(task, true);
            assertEquals(finalReport.status(), "failed");
            assertTrue(finalReport.resultReason().contains("replayed"));
            task.getRunning().set(false);
            // Ordinary grouped checkpoints retain their at-least-once semantics.
            proof.getPulsar().setCheckpointRestartMessages(null);
            assertEquals(task.getReport().status(), "passed");
        } finally {
            task.getExecutor().shutdownNow();
            workers.stop();
        }
    }
    private static MessageMetadata position(long segment, long entry) {
        return new MessageMetadata(null, 10L, entry, null, null, null, null, segment);
    }

}
