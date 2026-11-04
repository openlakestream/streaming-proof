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

/** Exact acknowledgment counts for a fixed-window P99 threshold decision. */
public record PublishLatencyWindowSnapshot(long count, long overLimitCount, long limitMillis) {
    public PublishLatencyWindowSnapshot {
        if (count < 0 || overLimitCount < 0 || overLimitCount > count || limitMillis <= 0) {
            throw new IllegalArgumentException("Invalid publish latency window counts or threshold");
        }
    }

    /** Equivalent to nearest-rank P99 > limit, without sampling or floating-point rounding. */
    public boolean exceedsP99Limit() {
        return count > 0 && overLimitCount > count / 100;
    }
}
