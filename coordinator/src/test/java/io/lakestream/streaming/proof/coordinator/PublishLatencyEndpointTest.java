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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.javalin.Javalin;
import io.lakestream.streaming.proof.common.WorkerHttpClient;
import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import io.lakestream.streaming.proof.worker.Worker;
import io.lakestream.streaming.proof.worker.WorkerHandler;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.testng.annotations.Test;

public class PublishLatencyEndpointTest {
    @Test
    public void testClosedBucketEndpointIsIdempotentAndMissingProducerFails() throws Exception {
        Worker worker = mock(Worker.class);
        LatencyMetricSnapshot snapshot = new LatencyMetricSnapshot(1, 16000, 16000, List.of(16000L));
        doReturn(snapshot).when(worker).publishLatencyWindowSnapshot("p", 42L);
        Javalin app = Javalin.create();
        new WorkerHandler(app, worker);
        try (AsyncHttpClient http = Dsl.asyncHttpClient()) {
            app.start(0);
            WorkerHttpClient client = new WorkerHttpClient(http, "http://localhost:" + app.port());
            assertEquals(client.publishLatencyWindow("p", 42).join(), snapshot);
            assertEquals(client.publishLatencyWindow("p", 42).join(), snapshot);
            verify(worker, never()).windowedMetricsSnapshot(anyString());
            CompletionException error = expectThrows(CompletionException.class,
                    () -> client.publishLatencyWindow("missing", 42).join());
            assertTrue(error.getCause().getMessage().contains("404"));
        } finally {
            app.stop();
        }
    }
}
