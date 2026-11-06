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

import io.lakestream.streaming.proof.common.records.PublishLatencyWindowSnapshot;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** UTC-aligned exact acknowledgment counters, independent of when metrics are read. */
final class FixedWindowLatencyRecorder {
    private static final int RETAINED_WINDOWS = 4;
    private final long windowMillis;
    private final long limitMillis;
    private final NavigableMap<Long, long[]> windows = new TreeMap<>();

    FixedWindowLatencyRecorder(int windowSeconds, long limitMillis) {
        if (windowSeconds <= 0 || limitMillis <= 0) {
            throw new IllegalArgumentException("Window seconds and threshold must be positive.");
        }
        windowMillis = TimeUnit.SECONDS.toMillis(windowSeconds);
        this.limitMillis = limitMillis;
    }

    synchronized void record(long latencyMillis, long acknowledgedAtMillis) {
        long windowId = acknowledgedAtMillis / windowMillis;
        long[] counts = windows.computeIfAbsent(windowId, ignored -> new long[2]);
        counts[0]++;
        if (latencyMillis > limitMillis) {
            counts[1]++;
        }
        while (windows.size() > RETAINED_WINDOWS) {
            windows.pollFirstEntry();
        }
    }

    synchronized PublishLatencyWindowSnapshot snapshot(long windowId) {
        long[] counts = windows.get(windowId);
        return counts == null ? new PublishLatencyWindowSnapshot(0, 0, limitMillis)
                : new PublishLatencyWindowSnapshot(counts[0], counts[1], limitMillis);
    }
}
