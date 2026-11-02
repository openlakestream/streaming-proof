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
package io.lakestream.streaming.proof.common.records;

import static org.testng.Assert.assertEquals;

import io.lakestream.streaming.proof.common.Util;
import org.testng.annotations.Test;

public class ProofPublishLatencyConfigTest {
    @Test
    public void testDefaults() throws Exception {
        Proof proof = Util.JSON_MAPPER.readValue("{}", Proof.class);
        assertEquals(proof.getPublishLatencyP99LimitMillis(), 0L);
        assertEquals(proof.getPublishLatencyWindowSeconds(), 60);
        assertEquals(Proof.builder().build().getPublishLatencyWindowSeconds(), 60);
        assertEquals(proof.getPublishLatencyConsecutiveWindows(), 2);
        assertEquals(Proof.builder().build().getPublishLatencyP99LimitMillis(), 0L);
        assertEquals(Proof.builder().build().getPublishLatencyConsecutiveWindows(), 2);
    }

    @Test
    public void testProducerRequestDefaultsToDisabled() throws Exception {
        NewProducers request = Util.JSON_MAPPER.readValue("{\"id\":\"p\"}", NewProducers.class);
        assertEquals(request.publishLatencyWindowSeconds(), 0);
        NewProducers enabled = new NewProducers("p", "t", 1, 1, 1, 8, "pulsar", null, false, 60);
        NewProducers restored = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsString(enabled),
                NewProducers.class);
        assertEquals(restored.publishLatencyWindowSeconds(), 60);
    }

    @Test
    public void testRoundTrip() throws Exception {
        Proof proof = Proof.builder().publishLatencyP99LimitMillis(15000)
                .publishLatencyWindowSeconds(30).publishLatencyConsecutiveWindows(3).build();
        Proof restored = Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsString(proof), Proof.class);
        assertEquals(restored.getPublishLatencyP99LimitMillis(), 15000L);
        assertEquals(restored.getPublishLatencyWindowSeconds(), 30);
        assertEquals(restored.getPublishLatencyConsecutiveWindows(), 3);
    }
}
