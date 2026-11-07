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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.Util;
import org.testng.annotations.Test;

public class ProofCleanupConfigTest {
    @Test
    public void testCleanupIsOptIn() throws Exception {
        assertFalse(Proof.builder().build().isCleanupTopicsOnSuccess());
        assertFalse(Util.JSON_MAPPER.readValue("{}", Proof.class).isCleanupTopicsOnSuccess());
        Proof enabled = Util.JSON_MAPPER.readValue("{\"cleanupTopicsOnSuccess\":true}", Proof.class);
        assertTrue(enabled.isCleanupTopicsOnSuccess());
        assertTrue(Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsString(enabled), Proof.class)
                .isCleanupTopicsOnSuccess());
    }
}
