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

import io.lakestream.streaming.proof.common.records.LatencyMetricSnapshot;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** UTC-aligned tumbling buckets, independent of when metrics are read. */
final class FixedWindowLatencyRecorder {
    private static final int RETAINED_WINDOWS = 4;
    private final long windowMillis;
    private final NavigableMap<Long, LatencyRecorder> windows = new TreeMap<>();

    FixedWindowLatencyRecorder(int windowSeconds) {
        if (windowSeconds <= 0) {
            throw new IllegalArgumentException("Window seconds must be positive.");
        }
        windowMillis = TimeUnit.SECONDS.toMillis(windowSeconds);
    }

    synchronized void record(long latencyMillis, long acknowledgedAtMillis) {
        long windowId = acknowledgedAtMillis / windowMillis;
        windows.computeIfAbsent(windowId, ignored -> new LatencyRecorder()).record(latencyMillis);
        while (windows.size() > RETAINED_WINDOWS) {
            windows.pollFirstEntry();
        }
    }

    synchronized LatencyMetricSnapshot snapshot(long windowId) {
        LatencyRecorder recorder = windows.get(windowId);
        return recorder == null ? new LatencyMetricSnapshot(0, 0, 0, List.of()) : recorder.snapshot();
    }
}
