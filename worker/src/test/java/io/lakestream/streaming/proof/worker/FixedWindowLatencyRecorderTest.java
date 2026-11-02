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
package io.lakestream.streaming.proof.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import io.lakestream.streaming.proof.common.ProofValue;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class FixedWindowLatencyRecorderTest {
    @Test
    public void testExactBoundaryAndIdempotentReads() {
        FixedWindowLatencyRecorder recorder = new FixedWindowLatencyRecorder(60);
        recorder.record(100, 59999);
        recorder.record(16000, 60000);
        recorder.record(17000, 119999);
        recorder.record(200, 120000);
        assertEquals(recorder.snapshot(0).count(), 1L);
        assertEquals(recorder.snapshot(1).count(), 2L);
        assertEquals(recorder.snapshot(1).maxMillis(), 17000L);
        assertEquals(recorder.snapshot(1), recorder.snapshot(1));
        assertEquals(recorder.snapshot(2).count(), 1L);
        assertEquals(recorder.snapshot(3).count(), 0L);
    }

    @Test
    public void testDelayedReadsDoNotCombineBucketsAndRetentionIsBounded() {
        FixedWindowLatencyRecorder recorder = new FixedWindowLatencyRecorder(1);
        for (int i = 0; i < 10; i++) {
            recorder.record(i, i * 1000L);
        }
        assertEquals(recorder.snapshot(5).count(), 0L);
        for (int i = 6; i < 10; i++) {
            assertEquals(recorder.snapshot(i).count(), 1L);
            assertEquals(recorder.snapshot(i).maxMillis(), (long) i);
        }
    }

    @Test
    public void testSamplesRemainBoundedWithinABucket() {
        FixedWindowLatencyRecorder recorder = new FixedWindowLatencyRecorder(60);
        for (int i = 0; i < 10000; i++) {
            recorder.record(i, 1000);
        }
        assertEquals(recorder.snapshot(0).count(), 10000L);
        assertTrue(recorder.snapshot(0).samplesMillis().size() <= 4096);
    }

    @Test
    public void testChartReadDoesNotResetFixedSamples() throws Exception {
        FixedWindowLatencyRecorder recorder = new FixedWindowLatencyRecorder(60);
        ProofProducer producer = new ProofProducer() {
            @Override
            public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
                return CompletableFuture.completedFuture(new MessageMetadata(value));
            }

            @Override
            public void close() {
            }
        };
        try (ProofProducerTask task = new ProofProducerTask(producer, 1, ProofValue.MIN_SIZE, recorder)) {
            task.sendAsync().join();
            assertEquals(task.getPublishLatencyWindowSnapshot().count(), 1L);
            assertEquals(task.getPublishLatencyWindowSnapshot().count(), 0L);
            long windowId = System.currentTimeMillis() / 60000L;
            // The acknowledgment can straddle a minute boundary.
            long count = recorder.snapshot(windowId).count() + recorder.snapshot(windowId - 1).count();
            assertEquals(count, 1L);
            assertEquals(task.getPublishLatencySnapshot().count(), 1L);
        }
    }
}
