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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.ConsumerCheckPoint;
import io.lakestream.streaming.proof.common.records.ProducerCheckpoint;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import io.lakestream.streaming.proof.common.records.PublishLatencyWindowSnapshot;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PublishLatencyRegressionTest {
    @DataProvider(name = "populations")
    public Object[][] populations() {
        return new Object[][] {
                {10000L, 100L, false},
                {10000L, 102L, true},
                {99L, 1L, false},
                {98L, 1L, true}
        };
    }

    @Test(dataProvider = "populations")
    public void testUnequalWorkerPopulationsUseExactAcknowledgmentCounts(long fast, long slow, boolean fails)
            throws Exception {
        ProofTask task = newTask();
        WorkerHttpClient fastWorker = mock(WorkerHttpClient.class);
        WorkerHttpClient slowWorker = mock(WorkerHttpClient.class);
        doReturn(CompletableFuture.completedFuture(new PublishLatencyWindowSnapshot(fast, 0, 15000)))
                .when(fastWorker).publishLatencyWindow(anyString(), anyLong());
        doReturn(CompletableFuture.completedFuture(new PublishLatencyWindowSnapshot(slow, slow, 15000)))
                .when(slowWorker).publishLatencyWindow(anyString(), anyLong());
        task.getClients().addAll(List.of(fastWorker, slowWorker));
        try {
            long first = startCollector(task);
            task.collectPublishLatencyWindow(first + 1);
            task.collectPublishLatencyWindow(first + 2);
            assertEquals(task.getPublishLatencyFailureReason() != null, fails);
        } finally {
            close(task);
        }
    }

    @Test
    public void testStopWaitsForPendingFinalClosedBucket() throws Exception {
        ProofTask task = newTask();
        CompletableFuture<PublishLatencyWindowSnapshot> pending = new CompletableFuture<>();
        CountDownLatch reading = new CountDownLatch(1);
        long latest = prepareStopScenario(task, pending, reading);
        Future<?> collecting = null;
        CompletableFuture<Void> stopping = null;
        try {
            collecting = task.getPublishLatencyExecutor().submit(() -> task.collectPublishLatencyWindow(latest));
            assertTrue(reading.await(2, TimeUnit.SECONDS));
            stopping = CompletableFuture.runAsync(task::stop);
            Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> task.getPublishLatencyExecutor().isShutdown());
            assertFalse(stopping.isDone());
            pending.complete(new PublishLatencyWindowSnapshot(100, 100, 15000));
            collecting.get(5, TimeUnit.SECONDS);
            stopping.get(5, TimeUnit.SECONDS);
            assertNotNull(task.getPublishLatencyFailureReason());
            assertEquals(task.getReport().status(), "failed");
        } finally {
            pending.complete(new PublishLatencyWindowSnapshot(100, 100, 15000));
            if (stopping != null) {
                stopping.get(10, TimeUnit.SECONDS);
            }
            close(task);
        }
    }

    @Test
    public void testInterruptedFinalCollectionRetriesWithoutLosingPreviousBreach() throws Exception {
        ProofTask task = newTask();
        CompletableFuture<PublishLatencyWindowSnapshot> pending = new CompletableFuture<>();
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        long latest = prepareStopScenario(task, pending, reading);
        try {
            Future<?> collecting = task.getPublishLatencyExecutor().submit(() -> {
                try {
                    task.collectPublishLatencyWindow(latest);
                } finally {
                    finished.countDown();
                }
            });
            assertTrue(reading.await(2, TimeUnit.SECONDS));
            collecting.cancel(true);
            assertTrue(finished.await(2, TimeUnit.SECONDS));
            assertEquals(task.getConsecutiveSlowPublishWindows(), 1);
            assertEquals(task.getLastPublishLatencyWindowId(), latest - 1);
            assertNull(task.getPublishLatencyFailureReason());
            task.stop();
            assertNotNull(task.getPublishLatencyFailureReason());
            assertEquals(task.getReport().status(), "failed");
        } finally {
            close(task);
        }
    }

    private static long prepareStopScenario(ProofTask task,
            CompletableFuture<PublishLatencyWindowSnapshot> pending, CountDownLatch reading) throws Exception {
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        AtomicInteger calls = new AtomicInteger();
        doAnswer(invocation -> {
            if (calls.incrementAndGet() == 2) {
                reading.countDown();
                return pending;
            }
            return CompletableFuture.completedFuture(new PublishLatencyWindowSnapshot(100, 100, 15000));
        }).when(client).publishLatencyWindow(anyString(), anyLong());
        ProofWorkerMetricsSnapshot empty = new ProofWorkerMetricsSnapshot(0, 0, 0, 0, 0, 0, null, null);
        doReturn(CompletableFuture.completedFuture(empty)).when(client).metrics(anyString());
        doReturn(CompletableFuture.completedFuture(empty)).when(client).metricsWindowed(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(client).stopProducers(anyString());
        doReturn(CompletableFuture.completedFuture(null)).when(client).stopAndRemoveConsumers(anyString());
        task.getClients().add(client);
        doReturn(Pair.of(new ProducerCheckpoint(), new ConsumerCheckPoint())).when(task).aggregateCheckpoints();
        startCollector(task);
        long latest = System.currentTimeMillis() / 60000L - 1;
        Field last = ProofTask.class.getDeclaredField("lastPublishLatencyWindowId");
        last.setAccessible(true);
        last.setLong(task, latest - 2);
        task.collectPublishLatencyWindow(latest - 1);
        assertEquals(task.getConsecutiveSlowPublishWindows(), 1);
        return latest;
    }

    private static long startCollector(ProofTask task) throws Exception {
        Method schedule = ProofTask.class.getDeclaredMethod("schedulePublishLatencyChecks");
        schedule.setAccessible(true);
        schedule.invoke(task);
        return task.getLastPublishLatencyWindowId();
    }

    private static ProofTask newTask() {
        Proof proof = Proof.builder().id("p").topic("t").features(List.of("at_least_once"))
                .finalWaitSeconds(0).publishLatencyP99LimitMillis(15000).build();
        return spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class)));
    }

    private static void close(ProofTask task) {
        task.getPublishLatencyExecutor().shutdownNow();
        task.getExecutor().shutdownNow();
    }
}
