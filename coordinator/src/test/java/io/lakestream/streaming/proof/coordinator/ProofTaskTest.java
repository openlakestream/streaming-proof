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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.lakestream.streaming.proof.common.LongSeq;
import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WebhookConfig;
import io.lakestream.streaming.proof.common.WebhookNotificationService;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.Drivers;
import io.lakestream.streaming.proof.common.records.KafkaProofConfig;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofClusterTarget;
import io.lakestream.streaming.proof.common.records.ProofPerformanceSummary;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.ProofSummary;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProofTaskTest {

    @Test
    public void testWriteDuplicateSummaryCountsInclusiveSequenceRanges() {
        ProofTask task = new ProofTask(Proof.builder().topic("duplicates").build(),
                new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
        try {
            ConsumerCheckPoint cp = task.getLastVerifiedConsumerCheckpoint();
            ConsumerCheckPoint.SeqRange consumed = new ConsumerCheckPoint.SeqRange();
            consumed.setStart(new LongSeq(0, MessageMetadata.empty()));
            consumed.setEnd(new LongSeq(20, MessageMetadata.empty()));
            cp.addKey("key", new TreeMap<>(Map.of("consumed", consumed)));
            ConsumerCheckPoint.SeqRange singleton = new ConsumerCheckPoint.SeqRange();
            singleton.setStart(new LongSeq(5, MessageMetadata.empty()));
            singleton.setEnd(new LongSeq(5, MessageMetadata.empty()));
            ConsumerCheckPoint.SeqRange multiple = new ConsumerCheckPoint.SeqRange();
            multiple.setStart(new LongSeq(10, MessageMetadata.empty()));
            multiple.setEnd(new LongSeq(12, MessageMetadata.empty()));
            cp.addWriteDupsOrOutOrder("key", new TreeMap<>(Map.of("single", singleton, "multiple", multiple)));
            assertEquals(task.getSummary().writeDuplicates(), 4L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCreateProofTopicsCreatesOutputTopicAcrossDistinctGeoDrivers() {
        ProofDriver adminDriver = mock(ProofDriver.class);
        ProofDriver consumerDriver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("geo-test.kafka.topic-a")
                .partitions(5)
                .features(List.of("at_least_once", "ordering"))
                .drivers(Drivers.builder()
                        .admin("kafka-geo-primary")
                        .producer("kafka-geo-primary")
                        .consumer("kafka-geo-secondary")
                        .build())
                .build();
        ProofTask task = new ProofTask(
                proof,
                new Configs(Map.of(), Map.of()),
                adminDriver,
                driverName -> "kafka-geo-secondary".equals(driverName) ? consumerDriver : adminDriver);
        try {
            task.createProofTopics();

            verify(adminDriver, times(1)).createTopic("geo-test.kafka.topic-a", 5, null);
            verify(consumerDriver, times(1)).createTopic("geo-test.kafka.topic-a", 5, null);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCreateProofTopicsPassesKafkaTopicConfigToAllTopics() {
        ProofDriver driver = mock(ProofDriver.class);
        Map<String, String> topicConfig = Map.of("unclean.leader.election.enable", "true");
        Proof proof = Proof.builder()
                .topic("kafka-topic")
                .partitions(5)
                .features(List.of("exactly_once"))
                .kafka(KafkaProofConfig.builder()
                        .topicConfig(topicConfig)
                        .build())
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.createProofTopics();

            verify(driver, times(1)).createTopic("kafka-topic", 5, topicConfig);
            verify(driver, times(1)).createTopic("kafka-topic_transactional", 5, topicConfig);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCreateProofTopicsCreatesScalableTopicWithInitialSegments() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("scalable-topic")
                .features(List.of("at_least_once", "ordering"))
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).initialSegments(4).build())
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.createProofTopics();

            verify(driver, times(1)).createTopic("topic://public/default/scalable-topic", 4, null);
            assertNull(proof.getPartitions());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesGeoTopicsAcrossDistinctDrivers() {
        ProofDriver adminDriver = mock(ProofDriver.class);
        ProofDriver consumerDriver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("geo-test.kafka.topic-b")
                .features(List.of("at_least_once", "ordering"))
                .drivers(Drivers.builder()
                        .admin("kafka-geo-primary")
                        .producer("kafka-geo-primary")
                        .consumer("kafka-geo-secondary")
                        .build())
                .build();
        ProofTask task = new ProofTask(
                proof,
                new Configs(Map.of(), Map.of()),
                adminDriver,
                driverName -> "kafka-geo-secondary".equals(driverName) ? consumerDriver : adminDriver);
        try {
            task.remove();

            verify(adminDriver, times(1)).deleteTopic("geo-test.kafka.topic-b");
            verify(consumerDriver, times(1)).deleteTopic("geo-test.kafka.topic-b");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesProofTopicForAtLeastOnceProof() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            verify(driver, never()).deleteTopic("test-topic_transactional");
            verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesProofAndTransactionalTopicsForExactlyOnceProof() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("exactly_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            InOrder inOrder = inOrder(driver);
            inOrder.verify(driver, times(1)).deleteTopic("test-topic_transactional");
            inOrder.verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesMainTopicWhenTransactionalCleanupFails() {
        ProofDriver driver = mock(ProofDriver.class);
        doThrow(new RuntimeException("missing topic"))
                .when(driver).deleteTopic("test-topic_transactional");
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("exactly_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            verify(driver, times(1)).deleteTopic("test-topic_transactional");
            verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeDetection() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof sharedProof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        ProofTask task = new ProofTask(sharedProof, new Configs(Map.of(), Map.of()), driver);
        try {
            assertTrue(task.isSharedMode());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testNonSharedModeByDefault() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            assertFalse(task.isSharedMode());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testGetSummaryVerifiedCountUsesSeqPlusOne() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            // Simulate 4 keys each with 125 messages (seq 0-124)
            ProducerCheckpoint producerCp = task.getLastVerifiedProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key2", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key3", new LongSeq(124, MessageMetadata.empty()));

            ProofSummary summary = task.getSummary();

            // With the fix: (124+1)*4 = 500, not 124*4 = 496
            assertEquals(summary.verified(), 500L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testGetSummaryVerifiedCountSingleKey() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            // Single key with 1000 messages (seq 0-999)
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key0", new LongSeq(999, MessageMetadata.empty()));

            ProofSummary summary = task.getSummary();
            assertEquals(summary.verified(), 1000L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReportPrefersFrozenClusterTargetSnapshotAfterStop() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Map<String, Object> brokerMetadata = new HashMap<>();
        brokerMetadata.put("replicas", 2);
        brokerMetadata.put("limits", new HashMap<>(Map.of("cpu", "2", "memory", "4Gi")));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("clusterResources", new HashMap<>(Map.of("broker", brokerMetadata)));

        Map<String, Object> driverConfigs = new HashMap<>();
        driverConfigs.put("pulsar.service.url", "pulsar://cluster-a:6650");
        driverConfigs.put("pulsar.admin.url", "http://cluster-a:8080");

        Configs configs = new Configs(
                Map.of(),
                Map.of("local_pulsar", new Driver("pulsar", driverConfigs, metadata)));
        Proof proof = Proof.builder()
                .topic("test-topic")
                .driver("local_pulsar")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, configs, driver);
        try {
            ProofClusterTarget liveTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(liveTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-a:6650");
            Map<String, Object> liveClusterResources =
                    (Map<String, Object>) liveTarget.metadata().get("clusterResources");
            Map<String, Object> liveBroker = (Map<String, Object>) liveClusterResources.get("broker");
            assertEquals(liveBroker.get("replicas"), 2);

            ((Map<String, Object>) brokerMetadata.get("limits")).put("memory", "8Gi");
            driverConfigs.put("pulsar.service.url", "pulsar://cluster-b:6650");

            ProofClusterTarget updatedLiveTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(updatedLiveTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-b:6650");
            Map<String, Object> updatedLiveClusterResources =
                    (Map<String, Object>) updatedLiveTarget.metadata().get("clusterResources");
            Map<String, Object> updatedLiveBroker = (Map<String, Object>) updatedLiveClusterResources.get("broker");
            Map<String, Object> updatedLiveLimits = (Map<String, Object>) updatedLiveBroker.get("limits");
            assertEquals(updatedLiveLimits.get("memory"), "8Gi");

            AtomicBoolean running = getRunningFlag(task);
            running.set(true);
            task.stop();

            ((Map<String, Object>) brokerMetadata.get("limits")).put("memory", "16Gi");
            driverConfigs.put("pulsar.service.url", "pulsar://cluster-c:6650");

            ProofClusterTarget frozenTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(frozenTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-b:6650");
            assertNotNull(frozenTarget.metadata());
            Map<String, Object> frozenClusterResources =
                    (Map<String, Object>) frozenTarget.metadata().get("clusterResources");
            Map<String, Object> frozenBroker = (Map<String, Object>) frozenClusterResources.get("broker");
            Map<String, Object> frozenLimits = (Map<String, Object>) frozenBroker.get("limits");
            assertEquals(frozenLimits.get("memory"), "8Gi");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationRunsFinalVerification() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            // stop() requires running=true to proceed past the CAS guard
            getRunningFlag(task).set(true);

            // Build producer checkpoint: 4 keys, each with seq 0-249 (250 msgs each)
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key2", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key3", new LongSeq(249, MessageMetadata.empty()));

            // Build consumer checkpoint: consumer has received all messages
            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1", "key2", "key3")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(249, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            // Invoke private completeProofAfterDuration via reflection
            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // After final verification, lastVerifiedProducerCheckpoint should be updated
            ProofSummary summary = task.getSummary();
            // (249+1) * 4 = 1000
            assertEquals(summary.verified(), 1000L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationWhenConsumerLagsBehind() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            getRunningFlag(task).set(true);

            // Simulate a prior verified checkpoint at seq 124 per key
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key0", new LongSeq(124, MessageMetadata.empty()));
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key1", new LongSeq(124, MessageMetadata.empty()));

            // Latest producer checkpoint has seq 249 but consumer only reached 200
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));

            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(200, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // Consumer hasn't caught up (200 < 249), so lastVerified stays at prior checkpoint
            ProofSummary summary = task.getSummary();
            // Prior checkpoint: (124+1) * 2 = 250
            assertEquals(summary.verified(), 250L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFinalVerificationUsesNewReportSetting() throws Exception {
        ProofTask task = newFinalVerificationTask(
                Proof.builder()
                        .id("test-proof")
                        .topic("test-topic")
                        .features(List.of("at_least_once", "ordering"))
                        .timeout(180)
                        .build(),
                Map.of("finalWaitSeconds", 0));

        try {
            invokeRunFinalVerification(task);
            verify(task, times(1)).aggregateCheckpoints();
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFinalVerificationProofSettingOverridesReportSetting() throws Exception {
        ProofTask task = newFinalVerificationTask(
                Proof.builder()
                        .id("test-proof")
                        .topic("test-topic")
                        .features(List.of("at_least_once", "ordering"))
                        .timeout(180)
                        .finalWaitSeconds(0)
                        .build(),
                Map.of("finalWaitSeconds", 5));

        try {
            invokeRunFinalVerification(task);
            verify(task, times(1)).aggregateCheckpoints();
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFinalVerificationUsesNewReportSettingBeforeLegacySetting() throws Exception {
        ProofTask task = newFinalVerificationTask(
                Proof.builder()
                        .id("test-proof")
                        .topic("test-topic")
                        .features(List.of("at_least_once", "ordering"))
                        .timeout(180)
                        .build(),
                Map.of("finalWaitSeconds", 0, "finalVerificationTimeoutSeconds", 5));

        try {
            invokeRunFinalVerification(task);
            verify(task, times(1)).aggregateCheckpoints();
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFinalVerificationSupportsLegacyReportSetting() throws Exception {
        ProofTask task = newFinalVerificationTask(
                Proof.builder()
                        .id("test-proof")
                        .topic("test-topic")
                        .features(List.of("at_least_once", "ordering"))
                        .timeout(180)
                        .build(),
                Map.of("finalVerificationTimeoutSeconds", 0));

        try {
            invokeRunFinalVerification(task);
            verify(task, times(1)).aggregateCheckpoints();
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    private static ProofTask newFinalVerificationTask(Proof proof, Map<String, Object> reportSettings) {
        ProofDriver driver = mock(ProofDriver.class);
        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of(), reportSettings), driver));

        ProducerCheckpoint producerCp = new ProducerCheckpoint();
        producerCp.addPublished("key0", new LongSeq(10, MessageMetadata.empty()));

        ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(new LongSeq(0, MessageMetadata.empty()));
        range.setEnd(new LongSeq(5, MessageMetadata.empty()));
        TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
        rangeMap.put("t0", range);
        consumerCp.addKey("key0", rangeMap);

        doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();
        return task;
    }

    private static void invokeRunFinalVerification(ProofTask task) throws Exception {
        Method method = ProofTask.class.getDeclaredMethod("runFinalVerification");
        method.setAccessible(true);
        method.invoke(task);
    }

    @Test
    public void testReportTimeSeriesIncludesCumulativeTimeouts() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .timeout(30)
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(15)));

        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);

        try {
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key0", new LongSeq(9, MessageMetadata.empty()));

            Field timeoutsField = ProofTask.class.getDeclaredField("timeouts");
            timeoutsField.setAccessible(true);
            timeoutsField.setInt(task, 2);

            Method recordTimeSeriesPoint = ProofTask.class.getDeclaredMethod("recordTimeSeriesPoint");
            recordTimeSeriesPoint.setAccessible(true);
            recordTimeSeriesPoint.invoke(task);

            ProofReport report = task.getReport();
            assertFalse(report.timeSeries().isEmpty());
            assertEquals(report.timeSeries().getLast().timeouts(), 2L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testManualStopReportsStoppedStatus() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .timeout(30)
                .build();

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            getRunningFlag(task).set(true);

            task.requestStop();

            assertEquals(task.getReport().status(), "stopped");
            assertEquals(task.getReport().resultReason(), "The run was stopped manually.");
            verify(task, never()).sendCompletionNotification(any());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testStartFailureMarksProofAsFailedInsteadOfRunning() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .build();

        RuntimeException startFailure = new RuntimeException("producer startup failed");
        doThrow(startFailure).when(driver).createTopic("test-topic", 10, null);

        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);

        try {
            try {
                task.start();
                fail("Expected start to fail");
            } catch (RuntimeException e) {
                assertEquals(e, startFailure);
            }

            assertFalse(task.isRunning());
            assertEquals(task.getReport().status(), "failed");
            assertEquals(task.getReport().resultReason(), "The run failed to start: producer startup failed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationSharedMode() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            assertTrue(task.isSharedMode());
            // stop() requires running=true to proceed past the CAS guard
            getRunningFlag(task).set(true);

            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));

            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(249, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // Shared mode: verified = sum of (watermark + 1)
            // Watermarks should be 249 per key → (249+1)*2 = 500
            ProofSummary summary = task.getSummary();
            assertEquals(summary.verified(), 500L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @DataProvider
    public Object[][] completionReportResults() {
        return new Object[][] {{0L, "passed"}, {20_000L, "failed"}};
    }

    @Test(dataProvider = "completionReportResults")
    public void testCompletionNotificationUsesFinalReport(long stallMillis, String expectedStatus) throws Exception {
        Proof proof = Proof.builder().id("test-proof").topic("test-topic")
                .features(List.of("at_least_once")).finalWaitSeconds(0).maxStallSeconds(10).build();
        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class)));
        try {
            getRunningFlag(task).set(true);
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - stallMillis);
            setLongField(task, "lastVerifiedObserved", 10L);
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(9, MessageMetadata.empty()));
            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
            range.setStart(new LongSeq(0, MessageMetadata.empty()));
            range.setEnd(new LongSeq(9, MessageMetadata.empty()));
            consumerCp.addKey("key0", Map.of("t0", range));
            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method complete = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            complete.setAccessible(true);
            complete.invoke(task);

            ArgumentCaptor<ProofReport> notification = ArgumentCaptor.forClass(ProofReport.class);
            verify(task).sendCompletionNotification(notification.capture());
            ProofReport report = notification.getValue();
            assertEquals(report.status(), expectedStatus);
            assertEquals(report.summary().verified(), 10L);
            assertEquals(report.summary().missed(), 0);
            assertEquals(report.status(), task.getReport().status());
            assertEquals(report.resultReason(), task.getReport().resultReason());
            assertEquals(report.summary(), task.getReport().summary());
            assertNotNull(report.completionTime());
            Field completionMillis = ProofTask.class.getDeclaredField("completionMillis");
            completionMillis.setAccessible(true);
            assertEquals(report.completionTime(), java.time.Instant.ofEpochMilli(completionMillis.getLong(task))
                    .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime()
                    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSlowWebhookDoesNotChangeFinalResultAndFinishesBeforeClientCloses() throws Exception {
        Proof proof = Proof.builder().id("test-proof").topic("test-topic")
                .features(List.of("at_least_once")).finalWaitSeconds(0).maxStallSeconds(1)
                .webhookConfig(WebhookConfig.of("https://example.com/webhook")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
        WebhookNotificationService webhook = mock(WebhookNotificationService.class);
        Field serviceField = ProofTask.class.getDeclaredField("webhookService");
        serviceField.setAccessible(true);
        ((WebhookNotificationService) serviceField.get(task)).close();
        serviceField.set(task, webhook);
        CompletableFuture<Void> delivery = new CompletableFuture<>();
        CountDownLatch sending = new CountDownLatch(1);
        AtomicReference<ProofReport> notified = new AtomicReference<>();
        doAnswer(invocation -> {
            notified.set(invocation.getArgument(0));
            sending.countDown();
            return delivery;
        }).when(webhook).sendProofCompletionNotification(any());
        CompletableFuture<Void> completion = null;
        try {
            getRunningFlag(task).set(true);
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis());
            Method complete = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            complete.setAccessible(true);
            completion = CompletableFuture.runAsync(() -> {
                try {
                    complete.invoke(task);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(sending.await(10, TimeUnit.SECONDS));
            assertTrue(task.isRunning());
            assertEquals(task.getReport().status(), "stopping");
            assertEquals(notified.get().status(), "passed");
            assertFalse(completion.isDone());
            verify(webhook, never()).close();
            // Cross the stall threshold while notification delivery is pending.
            Thread.sleep(2200);
            assertEquals(task.getReport().status(), "stopping");
            assertEquals(task.getReport().summary(), notified.get().summary());
            assertEquals(task.getReport().completionTime(), notified.get().completionTime());

            delivery.complete(null);
            completion.get(10, TimeUnit.SECONDS);
            assertEquals(task.getReport().status(), "passed");
            assertEquals(task.getReport().resultReason(), notified.get().resultReason());
            verify(webhook).close();
        } finally {
            delivery.complete(null);
            if (completion != null) {
                completion.get(10, TimeUnit.SECONDS);
            }
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeInFlightGapsAreNotMissed() throws Exception {
        ProofTask task = newSharedTask();
        try {
            sharedTick(task, 9, new long[]{0, 9});
            // 13 is still on its way to another consumer, not missed
            sharedTick(task, 19, new long[]{10, 12}, new long[]{14, 17});

            assertEquals(task.getSummary().verified(), 13L);
            assertEquals(task.getSummary().missed(), 0);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeCountsMessagesStillMissingAfterTimeout() throws Exception {
        ProofTask task = newSharedTask();
        try {
            sharedTick(task, 9, new long[]{0, 9});
            sharedTick(task, 19, new long[]{10, 12}, new long[]{14, 14}, new long[]{16, 17});

            // The in-check up to seq 19 times out: 15 arrived, 13 did not, 23-25 are past the in-check
            setLongField(task, "checkPointInCheckTimeStamps", System.currentTimeMillis() - 60_000L);
            sharedTick(task, 25, new long[]{14, 22});
            assertEquals(task.getTimeouts(), 1);
            assertEquals(task.getSummary().missed(), 1);

            sharedTick(task, 25, new long[]{13, 25});
            assertEquals(task.getSummary().missed(), 0);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeFinalVerificationReportsUnconsumedMessages() throws Exception {
        ProofTask task = newSharedTask();
        try {
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(19, MessageMetadata.empty()));
            // 13 and 19 never arrive
            doReturn(Pair.of(producerCp, consumed(new long[]{0, 12}, new long[]{14, 18})))
                    .when(task).aggregateCheckpoints();

            invokeRunFinalVerification(task);

            assertEquals(task.getSummary().missed(), 2);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeMissedIsClampedInsteadOfOverflowing() throws Exception {
        ProofTask task = newSharedTask();
        try {
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(Integer.MAX_VALUE, MessageMetadata.empty()));
            // Nothing consumed: Integer.MAX_VALUE + 1 messages are missing
            doReturn(Pair.of(producerCp, new ConsumerCheckPoint())).when(task).aggregateCheckpoints();

            invokeRunFinalVerification(task);

            assertEquals(task.getSummary().missed(), Integer.MAX_VALUE);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompletionNotificationCarriesFinalVerificationResult() throws Exception {
        ProofTask task = newSharedTask();
        try {
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(19, MessageMetadata.empty()));
            // No timeout fired during the run, and 13 and 19 never arrive
            doReturn(Pair.of(producerCp, consumed(new long[]{0, 12}, new long[]{14, 18})))
                    .when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            ArgumentCaptor<ProofReport> notified = ArgumentCaptor.forClass(ProofReport.class);
            verify(task).sendCompletionNotification(notified.capture());
            assertEquals(notified.getValue().summary().missed(), 2);
            assertEquals(notified.getValue().status(), "failed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSlowCompletionWebhookDoesNotCountAsStall() throws Exception {
        ProofTask task = newSharedTask();
        try {
            getRunningFlag(task).set(true);
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(9, MessageMetadata.empty()));
            doReturn(Pair.of(producerCp, consumed(new long[]{0, 9}))).when(task).aggregateCheckpoints();
            doAnswer(invocation -> {
                Thread.sleep(1_100); // a slow webhook endpoint
                return CompletableFuture.completedFuture(null);
            }).when(task).sendCompletionNotification(any());

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            assertEquals(task.getSummary().maxVerifiedStallSeconds(), 0L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    private static ProofTask newSharedTask() {
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .timeout(30)
                .finalWaitSeconds(0)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        return spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class)));
    }

    /** Runs one shared checkpoint verification with key0 published up to seq and consumed in ranges. */
    private static void sharedTick(ProofTask task, long seq, long[]... ranges) throws Exception {
        ProducerCheckpoint producerCp = new ProducerCheckpoint();
        producerCp.addPublished("key0", new LongSeq(seq, MessageMetadata.empty()));
        Field producerField = ProofTask.class.getDeclaredField("latestProducerCheckpoint");
        producerField.setAccessible(true);
        producerField.set(task, producerCp);
        task.setLatestConsumerCheckpoint(consumed(ranges));
        Method verifyShared = ProofTask.class.getDeclaredMethod("verifyShared");
        verifyShared.setAccessible(true);
        verifyShared.invoke(task);
    }

    private static ConsumerCheckPoint consumed(long[]... ranges) {
        TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
        for (int i = 0; i < ranges.length; i++) {
            ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
            range.setStart(new LongSeq(ranges[i][0], MessageMetadata.empty()));
            range.setEnd(new LongSeq(ranges[i][1], MessageMetadata.empty()));
            rangeMap.put("t" + i, range);
        }
        ConsumerCheckPoint checkpoint = new ConsumerCheckPoint();
        checkpoint.addKey("key0", rangeMap);
        return checkpoint;
    }

    @Test
    public void testVerifiedStallResetsOnIncrease() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 10_000L);
            setLongField(task, "lastVerifiedObserved", 100L);
            getRunningFlag(task).set(true);

            invokeTrackVerifiedStall(task, 101L); // increase by 1 -> reset

            assertTrue(task.getSummary().verifiedStallSeconds() <= 1L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testVerifiedStallDoesNotResetWhenUnchanged() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 10_000L);
            setLongField(task, "lastVerifiedObserved", 100L);
            getRunningFlag(task).set(true);

            invokeTrackVerifiedStall(task, 100L); // unchanged -> no reset

            assertTrue(task.getSummary().verifiedStallSeconds() >= 9L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testVerifiedStallDecreaseDoesNotResetButLaterRiseDoes() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 10_000L);
            setLongField(task, "lastVerifiedObserved", 100L);
            getRunningFlag(task).set(true);

            invokeTrackVerifiedStall(task, 90L); // decrease -> no reset, baseline drops to 90
            assertTrue(task.getSummary().verifiedStallSeconds() >= 9L);

            invokeTrackVerifiedStall(task, 95L); // rise above the new low point -> reset
            assertTrue(task.getSummary().verifiedStallSeconds() <= 1L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testVerifiedStallFreezesAfterCompletion() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            long now = System.currentTimeMillis();
            setLongField(task, "lastVerifiedChangeMillis", now - 30_000L);
            setLongField(task, "completionMillis", now - 20_000L); // completed 10s after last change
            getRunningFlag(task).set(false); // not running -> frozen

            long frozen = task.getSummary().verifiedStallSeconds();
            assertEquals(frozen, 10L);
            // Re-reading does not grow the frozen value
            assertEquals(task.getSummary().verifiedStallSeconds(), frozen);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testVerifiedStallAccumulatesFromStartWhenNeverVerified() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 15_000L);
            setLongField(task, "lastVerifiedObserved", 0L);
            getRunningFlag(task).set(true);

            invokeTrackVerifiedStall(task, 0L); // still 0 -> no reset

            assertTrue(task.getSummary().verifiedStallSeconds() >= 14L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testMaxStallCapturesGapBeforeResetThenSurvivesRecovery() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 20_000L);
            setLongField(task, "lastVerifiedObserved", 100L);
            getRunningFlag(task).set(true);

            invokeTrackVerifiedStall(task, 101L); // increase folds the 20s gap into the peak, then resets current

            ProofSummary summary = task.getSummary();
            assertTrue(summary.maxVerifiedStallSeconds() >= 19L); // peak retained
            assertTrue(summary.verifiedStallSeconds() <= 1L);     // current reset
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testMaxStallReflectsLiveGapWhileRunning() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            setLongField(task, "maxVerifiedStallMillis", 5_000L);
            setLongField(task, "lastVerifiedChangeMillis", System.currentTimeMillis() - 10_000L);
            getRunningFlag(task).set(true);

            // live gap (10s) exceeds the recorded peak (5s), so the read folds it in
            assertTrue(task.getSummary().maxVerifiedStallSeconds() >= 9L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testMaxStallFreezesAfterCompletion() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("test-topic").features(List.of("at_least_once")).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            long now = System.currentTimeMillis();
            setLongField(task, "maxVerifiedStallMillis", 30_000L);
            setLongField(task, "lastVerifiedChangeMillis", now - 1_000L);
            setLongField(task, "completionMillis", now);
            getRunningFlag(task).set(false); // frozen: live gap = now - (now-1000) = 1s, peak stays 30s

            assertEquals(task.getSummary().maxVerifiedStallSeconds(), 30L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testResultFailsWhenPeakStallExceedsLimit() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("t").features(List.of("at_least_once")).maxStallSeconds(180).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            getRunningFlag(task).set(false);
            ProofSummary summary = new ProofSummary(1000L, 0, 0, 0, 0L, 0L, 0, 0L, 240L);

            String status = invokeDetermineResultStatus(task, summary);
            assertEquals(status, "failed");
            String reason = invokeDetermineResultReason(task, summary, status);
            assertTrue(reason.contains("240s"));
            assertTrue(reason.contains("180s"));
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testResultPassesWhenStallLimitDisabled() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("t").features(List.of("at_least_once")).maxStallSeconds(0).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            getRunningFlag(task).set(false);
            ProofSummary summary = new ProofSummary(1000L, 0, 0, 0, 0L, 0L, 0, 0L, 9999L);

            assertEquals(invokeDetermineResultStatus(task, summary), "passed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testResultPassesWhenPeakStallWithinLimit() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("t").features(List.of("at_least_once")).maxStallSeconds(180).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            getRunningFlag(task).set(false);
            ProofSummary summary = new ProofSummary(1000L, 0, 0, 0, 0L, 0L, 0, 0L, 100L);

            assertEquals(invokeDetermineResultStatus(task, summary), "passed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testMissedReasonTakesPrecedenceOverStall() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder().topic("t").features(List.of("at_least_once")).maxStallSeconds(180).build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            getRunningFlag(task).set(false);
            ProofSummary summary = new ProofSummary(1000L, 0, 0, 5, 0L, 0L, 0, 0L, 240L);

            String status = invokeDetermineResultStatus(task, summary);
            assertEquals(status, "failed");
            String reason = invokeDetermineResultReason(task, summary, status);
            assertTrue(reason.contains("not verified")); // missed reason wins, not the stall reason
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    private static String invokeDetermineResultStatus(ProofTask task, ProofSummary summary) throws Exception {
        Method m = ProofTask.class.getDeclaredMethod(
                "determineResultStatus", ProofSummary.class, ProofPerformanceSummary.class, boolean.class);
        m.setAccessible(true);
        return (String) m.invoke(task, summary, null, false);
    }

    private static String invokeDetermineResultReason(ProofTask task, ProofSummary summary, String status)
            throws Exception {
        Method m = ProofTask.class.getDeclaredMethod(
                "determineResultReason", ProofSummary.class, ProofPerformanceSummary.class, String.class, boolean.class);
        m.setAccessible(true);
        return (String) m.invoke(task, summary, null, status, false);
    }

    private static void setLongField(ProofTask task, String fieldName, long value) throws Exception {
        Field field = ProofTask.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(task, value);
    }

    private static void invokeTrackVerifiedStall(ProofTask task, long verified) throws Exception {
        Method method = ProofTask.class.getDeclaredMethod("trackVerifiedStall", long.class);
        method.setAccessible(true);
        method.invoke(task, verified);
    }

    private static AtomicBoolean getRunningFlag(ProofTask task) throws Exception {
        Field runningField = ProofTask.class.getDeclaredField("running");
        runningField.setAccessible(true);
        return (AtomicBoolean) runningField.get(task);
    }
}
