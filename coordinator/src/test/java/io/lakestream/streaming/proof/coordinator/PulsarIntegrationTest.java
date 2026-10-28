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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.CoordinatorHttpClient;
import io.lakestream.streaming.proof.common.Util;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofDetails;
import io.lakestream.streaming.proof.common.records.PulsarProofConfig;
import io.lakestream.streaming.proof.worker.WorkerStarter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.awaitility.Awaitility;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarIntegrationTest {

    private CoordinatorStarter server;
    private WorkerStarter worker;
    private final AsyncHttpClient client = Dsl.asyncHttpClient();
    private CoordinatorHttpClient httpClient;
    private PulsarContainer pulsarContainer;
    private int workerPort;
    private int coordinatorPort;

    @BeforeClass
    public void setUp() throws Exception {
        int port = getFreePort();
        coordinatorPort = port;
        workerPort = getFreePort();

        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:5.0.0"));
        pulsarContainer.start();

        server = new CoordinatorStarter();
        worker = new WorkerStarter();
        server.start(port);
        worker.start(workerPort);
        this.httpClient = new CoordinatorHttpClient(client, "localhost", port);

        Configs configs = new Configs(
                Map.of("worker1", "http://localhost:" + workerPort),
                Map.of("pulsar_driver", new Driver("pulsar", Map.of(
                        "pulsar.service.url", pulsarContainer.getPulsarBrokerUrl(),
                        "pulsar.admin.url", pulsarContainer.getHttpServiceUrl()))));
        httpClient.putConfigs(configs).join();
    }

    @AfterClass
    public void tearDown() throws Exception {
        server.stop();
        worker.stop();
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testKeySharedSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Key_Shared"))
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testFailoverSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testExclusiveSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Exclusive"))
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testSharedSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(10)
                .partitions(1)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testFailoverWatermarkTrimming() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(3)
                .msgRate(500)
                .timeout(180)
                .build();
        runWatermarkTrimmingTest(proof);
    }

    @Test
    public void testKeySharedWatermarkTrimming() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(3)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Key_Shared"))
                        .build())
                .build();
        runWatermarkTrimmingTest(proof);
    }

    @Test
    public void testSharedSubscriptionWatermarkTrimming() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(10)
                .partitions(1)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once"))
                .checkPointInterval(3)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        runWatermarkTrimmingTest(proof);
    }

    @Test
    public void testScalableTopicStreamConsumer() throws Exception {
        // More consumers than segments, so one segment is shared by entry bucket
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .producers(2)
                .consumers(3)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).initialSegments(2).build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testScalableTopicQueueConsumer() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(10)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .scalableTopicEnabled(true)
                        .initialSegments(2)
                        .consumerType("queue")
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testScalableTopicCheckpointConsumer() throws Exception {
        // One consumer per segment: Pulsar 5.0.0 checkpoint consumers sharing a segment both read all of it
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .producers(2)
                .consumers(2)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .scalableTopicEnabled(true)
                        .initialSegments(2)
                        .consumerType("checkpoint")
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testScalableTopicSerializedCheckpointRecovery() throws Exception {
        int restartMessages = 20;
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(4).producers(1).consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(2).msgRate(40).timeout(30).duration(20).finalWaitSeconds(10)
                .pulsar(PulsarProofConfig.builder().scalableTopicEnabled(true).initialSegments(1)
                        .consumerType("checkpoint").checkpointRestartMessages(restartMessages).build())
                .build();
        httpClient.createProof(proof).join();
        String proofId = findProofId(proof.getName());
        try {
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > 3L * restartMessages,
                        "Verification must advance across several serialized reader restarts");
            });
            long firstVerified = httpClient.getProof(proofId).join().summary().verified();
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > firstVerified + 3L * restartMessages);
                assertEquals(details.summary().missed(), 0L);
                assertEquals(details.summary().duplicates(), 0L);
                assertEquals(details.summary().outOfOrders(), 0L);
            });
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                assertEquals(Util.JSON_MAPPER.readTree(client.prepareGet("http://localhost:" + coordinatorPort
                        + Util.GET_PROOF_REPORT.replace("{id}", proofId)).execute().get().getResponseBody())
                        .get("status").asText(), "passed");
            });
            ProofDetails details = httpClient.getProof(proofId).join();
            assertEquals(details.summary().missed(), 0L);
            assertEquals(details.summary().duplicates(), 0L);
            assertEquals(details.summary().outOfOrders(), 0L);
            assertEquals(details.summary().timeouts(), 0L);
        } finally {
            httpClient.deleteProof(proofId).join();
        }
    }

    private void runWatermarkTrimmingTest(Proof proof) throws Exception {
        httpClient.createProof(proof).join();
        String proofId = findProofId(proof.getName());
        try {
            // Wait for first successful verification
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > 0);
            });
            long firstVerified = httpClient.getProof(proofId).join().summary().verified();

            // Wait for verified count to grow across checkpoint cycles
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > firstVerified,
                        "Verified count should grow across checkpoint cycles, "
                                + "was " + firstVerified + " now " + details.summary().verified());
            });

            // Verify worker-side trimming: earliest range start should be above 0
            assertWorkerRangesTrimmed(proofId);
        } finally {
            httpClient.stopProof(proofId).join();
            httpClient.deleteProof(proofId).join();
        }
    }

    private void assertWorkerRangesTrimmed(String proofId) throws Exception {
        String url = "http://localhost:" + workerPort
                + Util.CONSUMER_CHECKPOINTS_DETAILS.replace("{id}", proofId);
        String body = client.prepareGet(url).execute().get().getResponseBody();
        @SuppressWarnings("unchecked")
        Map<String, ConsumerCheckPoint> details = Util.JSON_MAPPER.readValue(body,
                Util.JSON_MAPPER.getTypeFactory().constructMapType(
                        java.util.HashMap.class, String.class, ConsumerCheckPoint.class));
        boolean anyTrimmed = details.values().stream()
                .flatMap(cp -> cp.getConsumed().values().stream())
                .flatMap(rangeMap -> rangeMap.values().stream())
                .anyMatch(range -> range.getStart().seq() > 0);
        assertTrue(anyTrimmed,
                "Expected at least one consumer range to start above seq 0 after watermark trimming");
    }

    private void runProofAndVerify(Proof proof) throws Exception {
        httpClient.createProof(proof).join();
        String proofId = findProofId(proof.getName());
        try {
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > 0);
            });
        } finally {
            httpClient.stopProof(proofId).join();
            httpClient.deleteProof(proofId).join();
        }
    }

    private String findProofId(String name) {
        return Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> httpClient.listProofs().join(),
                        proofs -> proofs.stream().anyMatch(p -> name.equals(p.getName())))
                .stream()
                .filter(p -> name.equals(p.getName()))
                .findFirst()
                .orElseThrow()
                .getId();
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
