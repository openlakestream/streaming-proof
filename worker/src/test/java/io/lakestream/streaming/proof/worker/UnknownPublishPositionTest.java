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

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnknownPublishPositionTest {
    @DataProvider
    public Object[][] positions() {
        MessageMetadata first = new MessageMetadata(10L, 5L);
        MessageMetadata unknown = new MessageMetadata(-1L, -1L);
        return new Object[][] {
            {List.of(unknown), 0L},
            {List.of(first, unknown), 0L},
            {List.of(first, new MessageMetadata(-1L, -1L, 3), new MessageMetadata(10L, 6L)), 0L},
            {List.of(first, unknown, unknown, new MessageMetadata(10L, 6L)), 0L},
            {List.of(first, unknown, new MessageMetadata(10L, 4L)), 1L},
            {List.of(first, unknown, first), 1L},
            {List.of(new MessageMetadata(5L), new MessageMetadata(-1L), new MessageMetadata(6L)), 0L},
            {List.of(new MessageMetadata(5L), new MessageMetadata(-1L), new MessageMetadata(4L)), 1L},
            {List.of(new MessageMetadata(10L, 5L, 0), unknown, new MessageMetadata(10L, 5L, 1)), 0L},
            {List.of(new MessageMetadata(10L, 5L, 1), unknown, new MessageMetadata(10L, 5L, 0)), 1L},
            {List.of(first, new MessageMetadata(-1L, 4L), new MessageMetadata(10L, 6L)), 0L},
            {List.of(first, new MessageMetadata(10L, -1L), new MessageMetadata(10L, 6L)), 0L}
        };
    }

    @Test(dataProvider = "positions")
    public void unknownPositionsDoNotReportOrHideRegressions(List<MessageMetadata> positions, long expectedErrors)
            throws Exception {
        AtomicInteger index = new AtomicInteger();
        try (ProofProducerTask task = new ProofProducerTask(producer((key, seq) ->
                CompletableFuture.completedFuture(positions.get(index.getAndIncrement()))), 1, 1024)) {
            for (int i = 0; i < positions.size(); i++) {
                task.sendAsync().join();
            }
            var errors = task.getErrorDetails();
            assertEquals(errors.containsKey("Offset out of order writes")
                    ? errors.get("Offset out of order writes").getCount() : 0L, expectedErrors);
            assertEquals(errors.size(), expectedErrors == 0 ? 0 : 1);
            assertEquals(task.getAcknowledged(), positions.size());
            assertEquals(task.getLastPublished().values().iterator().next().seq(), positions.size() - 1L);
            assertEquals(task.getLastPublished().values().iterator().next().metadata(),
                    positions.get(positions.size() - 1));
        }
    }

    @Test
    public void delayedCompletionDoesNotReplaceNewerKnownPosition() throws Exception {
        List<CompletableFuture<MessageMetadata>> sends = new ArrayList<>();
        try (ProofProducerTask task = new ProofProducerTask(producer((key, seq) -> {
            var future = new CompletableFuture<MessageMetadata>();
            sends.add(future);
            return future;
        }), 1, 1024)) {
            for (int i = 0; i < 4; i++) {
                task.sendAsync();
            }
            sends.get(1).complete(new MessageMetadata(10L, 10L));
            sends.get(0).complete(new MessageMetadata(10L, 8L));
            assertEquals(task.getErrorDetails().size(), 0);
            sends.get(2).complete(new MessageMetadata(-1L, -1L));
            assertEquals(task.getErrorDetails().size(), 0);
            sends.get(3).complete(new MessageMetadata(10L, 9L));
            // Only the genuine position regression counts, not the delayed callback.
            assertEquals(task.getErrorDetails().get("Offset out of order writes").getCount(), 1L);
            assertEquals(task.getLastPublished().values().iterator().next().seq(), 3L);
        }
    }

    private static ProofProducer producer(java.util.function.BiFunction<String, Long,
            CompletableFuture<MessageMetadata>> send) {
        return new ProofProducer() {
            @Override
            public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
                return send.apply(key, value);
            }

            @Override
            public void close() {
            }
        };
    }
}
