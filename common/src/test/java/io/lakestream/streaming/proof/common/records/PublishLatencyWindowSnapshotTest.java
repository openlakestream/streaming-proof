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
import static org.testng.Assert.expectThrows;

import io.lakestream.streaming.proof.common.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PublishLatencyWindowSnapshotTest {
    @DataProvider(name = "counts")
    public Object[][] counts() {
        return new Object[][] {
                {0L, 0L, false}, {99L, 1L, true}, {100L, 1L, false},
                {10100L, 100L, false}, {10100L, 101L, false}, {10100L, 102L, true},
                {Long.MAX_VALUE, Long.MAX_VALUE / 100, false},
                {Long.MAX_VALUE, Long.MAX_VALUE / 100 + 1, true}
        };
    }

    @Test(dataProvider = "counts")
    public void testExactP99Threshold(long total, long slow, boolean exceeds) {
        assertEquals(new PublishLatencyWindowSnapshot(total, slow, 15000).exceedsP99Limit(), exceeds);
    }

    @Test
    public void testWireCountsAndRejectInvalidOrLegacyPayload() throws Exception {
        PublishLatencyWindowSnapshot snapshot = new PublishLatencyWindowSnapshot(10100, 100, 15000);
        assertEquals(Util.JSON_MAPPER.readValue(Util.JSON_MAPPER.writeValueAsString(snapshot),
                PublishLatencyWindowSnapshot.class), snapshot);
        expectThrows(Exception.class, () -> Util.JSON_MAPPER.readValue(
                "{\"count\":100,\"sumMillis\":100,\"maxMillis\":1,\"samplesMillis\":[1]}",
                PublishLatencyWindowSnapshot.class));
        expectThrows(IllegalArgumentException.class, () -> new PublishLatencyWindowSnapshot(1, 2, 15000));
    }
}
