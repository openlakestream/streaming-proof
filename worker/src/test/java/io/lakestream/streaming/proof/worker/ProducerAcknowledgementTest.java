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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.testng.annotations.Test;

public class ProducerAcknowledgementTest {
    @Test
    public void synchronousFailureIsRecordedWithoutEscapingTheSendLoop() throws Exception {
        try (ProofProducerTask task = task((key, seq) -> {
            if (seq == 1) {
                throw new IllegalStateException("sync failure");
            }
            return CompletableFuture.completedFuture(new MessageMetadata(seq));
        })) {
            task.sendAsync().join();
            var failure = task.sendAsync();
            assertThrows(CompletionException.class, failure::join);
            task.sendAsync().join();
            assertEquals(watermark(task), 0L);
            assertEquals(task.getSendAttempts(), 3L);
            assertEquals(task.getAcknowledged(), 2L);
            assertEquals(task.getSendFailures(), 1L);
            assertEquals(task.getErrorDetails().get("sync failure").getCount(), 1L);
        }
    }

    @Test
    public void asynchronousFailureCannotBeHiddenByLaterSuccesses() throws Exception {
        List<CompletableFuture<MessageMetadata>> sends = new ArrayList<>();
        try (ProofProducerTask task = task((key, seq) -> {
            var future = new CompletableFuture<MessageMetadata>();
            sends.add(future);
            return future;
        })) {
            for (int i = 0; i < 4; i++) {
                task.sendAsync();
            }
            sends.get(3).complete(new MessageMetadata(3L));
            sends.get(1).completeExceptionally(new IllegalStateException("async failure"));
            sends.get(0).complete(new MessageMetadata(0L));
            sends.get(2).complete(new MessageMetadata(2L));
            assertEquals(watermark(task), 0L);
            assertEquals(task.getAcknowledged(), 3L);
            assertEquals(task.getSendFailures(), 1L);
            assertEquals(task.getErrorDetails().size(), 1);
        }
    }

    @Test
    public void outOfOrderAcknowledgementsAdvanceOnlyAContiguousWatermark() throws Exception {
        List<CompletableFuture<MessageMetadata>> sends = new ArrayList<>();
        try (ProofProducerTask task = task((key, seq) -> {
            var future = new CompletableFuture<MessageMetadata>();
            sends.add(future);
            return future;
        })) {
            for (int i = 0; i < 4; i++) {
                task.sendAsync();
            }
            sends.get(3).complete(new MessageMetadata(3L));
            sends.get(1).complete(new MessageMetadata(1L));
            assertEquals(watermark(task), -1L);
            sends.get(0).complete(new MessageMetadata(0L));
            assertEquals(watermark(task), 1L);
            sends.get(2).complete(new MessageMetadata(2L));
            assertEquals(watermark(task), 3L);
            assertTrue(task.getErrorDetails().isEmpty());
        }
    }

    @Test
    public void acknowledgementProcessingFailureIsVisibleEvenWhenTheFutureIsIgnored() throws Exception {
        try (ProofProducerTask task = task((key, seq) -> CompletableFuture.completedFuture(
                seq == 1 ? null : new MessageMetadata(seq)))) {
            task.sendAsync().join();
            var invalid = task.sendAsync();
            assertThrows(CompletionException.class, invalid::join);
            task.sendAsync().join();
            assertEquals(watermark(task), 0L);
            assertEquals(task.getAcknowledged(), 3L);
            assertEquals(task.getSendFailures(), 0L);
            assertEquals(task.getErrorDetails().get("Producer acknowledgement processing failed").getCount(), 1L);
        }
    }

    @Test
    public void nullFutureIsATerminalSendFailure() throws Exception {
        try (ProofProducerTask task = task((key, seq) -> null)) {
            assertThrows(CompletionException.class, () -> task.sendAsync().join());
            assertEquals(watermark(task), -1L);
            assertEquals(task.getSendFailures(), 1L);
            assertEquals(task.getAcknowledged(), 0L);
        }
    }

    @Test
    public void nullExceptionMessageStillProducesAnErrorCheckpoint() throws Exception {
        try (ProofProducerTask task = task((key, seq) ->
                CompletableFuture.failedFuture(new IllegalStateException()))) {
            assertThrows(CompletionException.class, () -> task.sendAsync().join());
            assertEquals(task.getErrorDetails().get(IllegalStateException.class.getName()).getCount(), 1L);
        }
    }

    @Test
    public void checkpointSnapshotDoesNotChangeAfterLaterAcknowledgements() throws Exception {
        try (ProofProducerTask task = task((key, seq) ->
                CompletableFuture.completedFuture(new MessageMetadata(seq)))) {
            task.sendAsync().join();
            var snapshot = task.getLastPublished();
            task.sendAsync().join();
            assertEquals(snapshot.values().iterator().next().seq(), 0L);
            assertEquals(watermark(task), 1L);
        }
    }

    @Test
    public void positionValidationErrorsAreNotCountedAsSendFailures() throws Exception {
        try (ProofProducerTask task = task((key, seq) ->
                CompletableFuture.completedFuture(new MessageMetadata(10 - seq)))) {
            task.sendAsync().join();
            task.sendAsync().join();
            assertEquals(task.getAcknowledged(), 2L);
            assertEquals(task.getSendFailures(), 0L);
            assertEquals(task.getErrorDetails().get("Offset out of order writes").getCount(), 1L);
        }
    }

    private static long watermark(ProofProducerTask task) {
        return task.getLastPublished().values().iterator().next().seq();
    }

    private static ProofProducerTask task(BiFunction<String, Long, CompletableFuture<MessageMetadata>> send) {
        return new ProofProducerTask(new ProofProducer() {
            @Override
            public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
                return send.apply(key, value);
            }

            @Override
            public void close() {
            }
        }, 1, 8);
    }
}
