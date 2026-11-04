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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.lakestream.streaming.proof.common.ProofDriver;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.Driver;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofWorkerMetricsSnapshot;
import io.lakestream.streaming.proof.common.records.PublishLatencyWindowSnapshot;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PublishLatencyTest {
    @DataProvider(name = "windows")
    public Object[][] windows() {
        return new Object[][] {
                {0L, 2, new long[] {16000, 17000}, "passed"},
                {15000L, 2, new long[] {16000}, "passed"},
                {15000L, 2, new long[] {15000, 15000}, "passed"},
                {15000L, 2, new long[] {16000, 17000}, "failed"},
                {15000L, 2, new long[] {16000, 100, 17000}, "passed"},
                {15000L, 2, new long[] {16000, 0, 17000}, "passed"},
                {15000L, 2, new long[] {16000, 17000, 100}, "failed"},
                {15000L, 1, new long[] {16000}, "failed"},
                {15000L, 3, new long[] {16000, 17000}, "passed"}
        };
    }

    @Test(dataProvider = "windows")
    public void testConsecutiveWindows(long limit, int consecutive, long[] windows, String expected) {
        ProofTask task = newTask(limit, consecutive);
        try {
            for (long p99 : windows) {
                task.recordPublishLatencyWindow(p99 == 0 ? null : latency(p99));
            }
            assertEquals(task.getReport().status(), expected);
            if ("failed".equals(expected)) {
                assertTrue(task.getReport().resultReason().contains(limit + "ms"));
                assertTrue(task.getReport().resultReason().contains(consecutive + " consecutive"));
            }
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testEmptyWindowResetsStreak() {
        ProofTask task = newTask(15000, 2);
        try {
            task.recordPublishLatencyWindow(latency(16000));
            task.recordPublishLatencyWindow(new PublishLatencyWindowSnapshot(0, 0, 15000));
            task.recordPublishLatencyWindow(latency(16000));
            assertEquals(task.getReport().status(), "passed");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRunningAndManualStopStatusesArePreserved() {
        ProofTask task = newTask(15000, 1);
        try {
            task.recordPublishLatencyWindow(latency(16000));
            task.getRunning().set(true);
            assertEquals(task.getReport().status(), "running");
            task.getRunning().set(false);
            task.getManuallyStopped().set(true);
            assertEquals(task.getReport().status(), "stopped");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFixedBucketsIgnoreChartSamplingAndDuplicateReads() throws Exception {
        ProofTask task = newTask(15000, 2);
        Proof proof = task.getProof();
        proof.setCheckPointInterval(5);
        proof.setTimeSeriesInterval(600);
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        PublishLatencyWindowSnapshot slow = new PublishLatencyWindowSnapshot(100, 100, 15000);
        ProofWorkerMetricsSnapshot fast = new ProofWorkerMetricsSnapshot(
                10000, 10000, 10000, 0, 10000, 10000,
                new LatencyMetricSnapshot(10000, 10000, 16000, List.of(1L)), null);
        doReturn(CompletableFuture.completedFuture(fast)).when(client).metrics(anyString());
        doReturn(CompletableFuture.completedFuture(fast)).when(client).metricsWindowed(anyString());
        doReturn(CompletableFuture.completedFuture(slow)).when(client).publishLatencyWindow(anyString(), anyLong());
        task.getClients().add(client);
        try {
            Method schedule = ProofTask.class.getDeclaredMethod("schedulePublishLatencyChecks");
            schedule.setAccessible(true);
            schedule.invoke(task);
            long startupWindow = task.getLastPublishLatencyWindowId();
            task.collectPublishLatencyWindow(startupWindow); // Partial startup bucket is excluded.
            assertEquals(task.getReport().status(), "passed");
            task.collectPublishLatencyWindow(startupWindow + 1);
            task.collectPublishLatencyWindow(startupWindow + 1); // Read is idempotent.
            assertEquals(task.getReport().status(), "passed");
            proof.setStartTime(LocalDateTime.now().minusSeconds(700).toString());
            Method chart = ProofTask.class.getDeclaredMethod("recordTimeSeriesPoint");
            chart.setAccessible(true);
            chart.invoke(task); // A fast chart window must not reset the fixed-window streak.
            proof.setTimeSeriesInterval(3600);
            task.collectPublishLatencyWindow(startupWindow + 2);
            assertEquals(task.getReport().status(), "failed");
            assertEquals(task.getReport().performanceSummary().publishLatency().p99(), 1.0d);
        } finally {
            task.getPublishLatencyExecutor().shutdownNow();
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testMissingBucketBreaksConsecutiveWindows() throws Exception {
        ProofTask task = newTask(15000, 2);
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        PublishLatencyWindowSnapshot slow = new PublishLatencyWindowSnapshot(100, 100, 15000);
        doReturn(CompletableFuture.completedFuture(slow)).when(client).publishLatencyWindow(anyString(), anyLong());
        task.getClients().add(client);
        try {
            Method schedule = ProofTask.class.getDeclaredMethod("schedulePublishLatencyChecks");
            schedule.setAccessible(true);
            schedule.invoke(task);
            long startupWindow = task.getLastPublishLatencyWindowId();
            task.collectPublishLatencyWindow(startupWindow + 1);
            task.collectPublishLatencyWindow(startupWindow + 3); // One full window was missed.
            assertEquals(task.getPublishLatencyFailureReason(), null);
            task.collectPublishLatencyWindow(startupWindow + 4);
            assertTrue(task.getPublishLatencyFailureReason().contains("2 consecutive fixed windows"));
        } finally {
            task.getPublishLatencyExecutor().shutdownNow();
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testUnavailableWorkerBreaksStreak() throws Exception {
        ProofTask task = newTask(15000, 2);
        WorkerHttpClient client = mock(WorkerHttpClient.class);
        PublishLatencyWindowSnapshot slow = new PublishLatencyWindowSnapshot(100, 100, 15000);
        CompletableFuture<PublishLatencyWindowSnapshot> failed = CompletableFuture.failedFuture(
                new IllegalStateException("worker unavailable"));
        doReturn(CompletableFuture.completedFuture(slow), failed, CompletableFuture.completedFuture(slow))
                .when(client).publishLatencyWindow(anyString(), anyLong());
        task.getClients().add(client);
        try {
            Method schedule = ProofTask.class.getDeclaredMethod("schedulePublishLatencyChecks");
            schedule.setAccessible(true);
            schedule.invoke(task);
            long startupWindow = task.getLastPublishLatencyWindowId();
            task.collectPublishLatencyWindow(startupWindow + 1);
            task.collectPublishLatencyWindow(startupWindow + 2);
            task.collectPublishLatencyWindow(startupWindow + 3);
            assertEquals(task.getConsecutiveSlowPublishWindows(), 1);
            // Avoid an unrelated report read through the mocked worker.
            assertEquals(task.getPublishLatencyFailureReason(), null);
        } finally {
            task.getPublishLatencyExecutor().shutdownNow();
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testStallFailureTakesPrecedence() throws Exception {
        ProofTask task = newTask(15000, 1);
        try {
            task.getProof().setMaxStallSeconds(10);
            java.lang.reflect.Field peak = ProofTask.class.getDeclaredField("maxVerifiedStallMillis");
            peak.setAccessible(true);
            peak.setLong(task, 20000L);
            task.recordPublishLatencyWindow(latency(16000));
            assertEquals(task.getReport().status(), "failed");
            assertTrue(task.getReport().resultReason().startsWith("Verification stalled"));
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @DataProvider(name = "invalidSettings")
    public Object[][] invalidSettings() {
        return new Object[][] {
                {-1L, 2, "Publish latency P99 limit must be 0 or greater."},
                {15000L, 0, "Publish latency consecutive windows must be at least 1."},
                {0L, -1, "Publish latency consecutive windows must be at least 1."}
        };
    }

    @Test(dataProvider = "invalidSettings")
    public void testRejectsInvalidSettings(long limit, int consecutive, String message) {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(Map.of(), Map.of("test", new Driver("pulsar", Map.of(), null))));
        Proof proof = Proof.builder().driver("test").topic("t").features(List.of("at_least_once"))
                .publishLatencyP99LimitMillis(limit).publishLatencyConsecutiveWindows(consecutive).build();
        assertEquals(expectThrows(IllegalArgumentException.class, () -> coordinator.createProof(proof)).getMessage(),
                message);
    }

    @Test
    public void testRejectsInvalidFixedWindow() {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(Map.of(), Map.of("test", new Driver("pulsar", Map.of(), null))));
        Proof proof = Proof.builder().driver("test").topic("t").features(List.of("at_least_once"))
                .publishLatencyWindowSeconds(0).build();
        assertEquals(expectThrows(IllegalArgumentException.class, () -> coordinator.createProof(proof)).getMessage(),
                "Publish latency window seconds must be at least 1.");
    }

    private static ProofTask newTask(long limit, int consecutive) {
        Proof proof = Proof.builder().id("p").topic("t").features(List.of("at_least_once"))
                .publishLatencyP99LimitMillis(limit).publishLatencyConsecutiveWindows(consecutive).build();
        return new ProofTask(proof, new Configs(Map.of(), Map.of()), mock(ProofDriver.class));
    }

    private static PublishLatencyWindowSnapshot latency(long p99) {
        return new PublishLatencyWindowSnapshot(100, p99 > 15000 ? 100 : 0, 15000);
    }
}
