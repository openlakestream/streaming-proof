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
package io.lakestream.streaming.proof.driver.pulsar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;

@Slf4j
public class OffloadCondition {

    private static final long DEFAULT_MAX_WAIT_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long DEFAULT_INITIAL_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long DEFAULT_MAX_RETRY_BACKOFF_MS = TimeUnit.SECONDS.toMillis(30);

    private final PulsarAdmin admin;
    private final String topic;
    private final boolean waitOffloadedForEachLedger;
    private final int waitOffloadedForCatchupRead;
    private final long maxWaitMs;
    private final long initialRetryBackoffMs;
    private final long maxRetryBackoffMs;
    private final boolean allowDegradedMode;
    private final Map<String, PersistentTopicInternalStats> topicInternalStatsCache = new LinkedHashMap<>();
    private final Set<Long> offloadedLedgersCache = new HashSet<>();
    private final Set<Long> degradedLedgersCache = new HashSet<>();
    private volatile boolean degradedMode;

    public static Optional<OffloadCondition> getOffloadCondition(
            PulsarAdmin admin, String topicName, Map<String, Object> configs) {
        if (configs == null || configs.isEmpty()) {
            return Optional.empty();
        }

        var waitOffloadedForEachLedger = (Boolean) configs
            .getOrDefault("offload.condition.waitOffloadedForEachLedger", false);
        var waitOffloadedForCatchupRead = (Integer) configs
            .getOrDefault("offload.condition.offloadedLedgersBeforeCatchupRead", 0);

        if (!waitOffloadedForEachLedger && waitOffloadedForCatchupRead == 0) {
            return Optional.empty();
        }

        return Optional.of(new OffloadCondition(
                admin,
                topicName,
                waitOffloadedForEachLedger,
                waitOffloadedForCatchupRead,
                getLongConfig(configs, "offload.condition.maxWaitMs", DEFAULT_MAX_WAIT_MS),
                getLongConfig(configs, "offload.condition.initialRetryBackoffMs", DEFAULT_INITIAL_RETRY_BACKOFF_MS),
                getLongConfig(configs, "offload.condition.maxRetryBackoffMs", DEFAULT_MAX_RETRY_BACKOFF_MS),
                getBooleanConfig(configs, "offload.condition.allowDegradedMode", true)));
    }

    public OffloadCondition(PulsarAdmin admin, String topic, boolean waitOffloadedForEachLedger,
                            int waitOffloadedForCatchupRead,
                            long maxWaitMs, long initialRetryBackoffMs,
                            long maxRetryBackoffMs, boolean allowDegradedMode) {
        this.admin = admin;
        this.topic = topic;
        this.waitOffloadedForEachLedger = waitOffloadedForEachLedger;
        this.waitOffloadedForCatchupRead = waitOffloadedForCatchupRead;
        this.maxWaitMs = maxWaitMs;
        this.initialRetryBackoffMs = Math.max(1L, initialRetryBackoffMs);
        this.maxRetryBackoffMs = Math.max(this.initialRetryBackoffMs, maxRetryBackoffMs);
        this.allowDegradedMode = allowDegradedMode;
    }

    public void waitOffloadConditionMeetForMessage(MessageIdAdv messageIdAdv) {
        if (!waitOffloadedForEachLedger) {
            return;
        }
        long ledgerId = messageIdAdv.getLedgerId();
        if (offloadedLedgersCache.contains(ledgerId) || degradedLedgersCache.contains(ledgerId)) {
            return;
        }
        log.info("Checking offload condition for topic: {}, ledgerId: {}", topic, ledgerId);
        boolean conditionMet = waitForCondition("ledger-" + ledgerId, () -> {
            Set<Long> offloadedLedgers = getOffloadedLedgers();
            offloadedLedgersCache.addAll(offloadedLedgers);
            return offloadedLedgers.contains(ledgerId);
        });
        if (conditionMet) {
            degradedLedgersCache.remove(ledgerId);
            unloadTopics();
        } else if (allowDegradedMode && degradedMode) {
            degradedLedgersCache.add(ledgerId);
        }
    }

    public void waitOffloadConditionMeetForCatchupRead(long ledgerId) {
        if (waitOffloadedForCatchupRead <= 0) {
            return;
        }
        if (offloadedLedgersCache.contains(ledgerId) || degradedLedgersCache.contains(ledgerId)) {
            return;
        }
        log.info("Waiting for the offload condition for topic: {} to start the catchup read", topic);
        boolean conditionMet = waitForCondition("catchup-read", this::isCatchupReadConditionMet);
        if (conditionMet) {
            Set<Long> offloadedLedgers = getOffloadedLedgers();
            unloadTopics();
            offloadedLedgersCache.addAll(offloadedLedgers);
        } else if (allowDegradedMode && degradedMode) {
            degradedLedgersCache.add(ledgerId);
        }
    }

    public boolean isDegradedMode() {
        return degradedMode;
    }

    private boolean waitForCondition(String conditionName, BooleanSupplier conditionSupplier) {
        long startTimeMs = System.currentTimeMillis();
        long retryBackoffMs = initialRetryBackoffMs;
        while (!conditionSupplier.getAsBoolean()) {
            RefreshResult refreshResult = refreshTopicInternalStats();
            if (refreshResult.failureCount > 0) {
                log.warn("Refresh topic internal stats had {} failures and {} successes for topic {}",
                        refreshResult.failureCount, refreshResult.successCount, topic);
            }

            if (conditionSupplier.getAsBoolean()) {
                degradedMode = false;
                return true;
            }

            if (hasExceededWaitTime(startTimeMs)) {
                if (allowDegradedMode) {
                    degradedMode = true;
                    log.warn("Offload condition '{}' timed out for topic {} after {} ms; entering degraded mode",
                            conditionName, topic, maxWaitMs);
                    return false;
                }
                log.warn("Offload condition '{}' timed out for topic {} after {} ms; keep waiting due to config",
                        conditionName, topic, maxWaitMs);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(retryBackoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                degradedMode = allowDegradedMode;
                log.warn("Interrupted while waiting for offload condition for topic: {}", topic);
                return false;
            }
            retryBackoffMs = Math.min(maxRetryBackoffMs, retryBackoffMs * 2);
        }

        degradedMode = false;
        return true;
    }

    private boolean hasExceededWaitTime(long startTimeMs) {
        return maxWaitMs > 0 && System.currentTimeMillis() - startTimeMs >= maxWaitMs;
    }

    private boolean isCatchupReadConditionMet() {
        if (topicInternalStatsCache.isEmpty()) {
            return false;
        }
        for (PersistentTopicInternalStats stats : topicInternalStatsCache.values()) {
            if (stats == null || stats.ledgers == null) {
                return false;
            }
            long newOffloadedLedgers = stats.ledgers.stream()
                    .filter(ledger -> ledger.offloaded)
                    .map(ledger -> ledger.ledgerId)
                    .filter(ledgerId -> !offloadedLedgersCache.contains(ledgerId))
                    .count();
            if (newOffloadedLedgers < waitOffloadedForCatchupRead) {
                return false;
            }
        }
        return true;
    }

    private Set<Long> getOffloadedLedgers() {
        return topicInternalStatsCache.values().stream()
                .filter(stats -> stats != null && stats.ledgers != null)
                .flatMap(stats -> stats.ledgers.stream())
                .filter(ledger -> ledger.offloaded)
                .map(ledger -> ledger.ledgerId)
                .collect(Collectors.toSet());
    }

    private void unloadTopics() {
        if (topicInternalStatsCache.isEmpty()) {
            refreshTopicInternalStats();
        }
        for (String partitionedTopic : topicInternalStatsCache.keySet()) {
            try {
                admin.topics().unload(partitionedTopic);
            } catch (Exception e) {
                log.warn("Failed to unload topic {} after offload condition met", partitionedTopic, e);
            }
        }
    }

    private RefreshResult refreshTopicInternalStats() {
        if (topicInternalStatsCache.isEmpty()) {
            try {
                PartitionedTopicMetadata topicMetadata = admin.topics().getPartitionedTopicMetadata(topic);
                if (topicMetadata.partitions > 0) {
                    for (int i = 0; i < topicMetadata.partitions; i++) {
                        topicInternalStatsCache.put(partitionName(topic, i), null);
                    }
                } else {
                    topicInternalStatsCache.put(topic, null);
                }
            } catch (Exception e) {
                log.warn("Failed to fetch partitioned metadata for topic {}", topic, e);
                return new RefreshResult(0, 1, Collections.emptyList());
            }
        }

        int successCount = 0;
        int failureCount = 0;
        List<String> failedTopics = new ArrayList<>();
        for (String partitionedTopic : topicInternalStatsCache.keySet()) {
            try {
                PersistentTopicInternalStats stats = admin.topics().getInternalStats(partitionedTopic);
                topicInternalStatsCache.put(partitionedTopic, stats);
                successCount++;
            } catch (Exception e) {
                failureCount++;
                failedTopics.add(partitionedTopic);
                log.warn("Failed to refresh topic internal stats for topic: {}", partitionedTopic, e);
            }
        }
        return new RefreshResult(successCount, failureCount, failedTopics);
    }

    /** Pulsar's partition naming; TopicName is relocated inside the shaded client jar. */
    static String partitionName(String topic, int partition) {
        return topic + "-partition-" + partition;
    }

    private static long getLongConfig(Map<String, Object> configs, String key, long defaultValue) {
        Object value = configs.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                log.warn("Invalid long config '{}'={}, using default {}", key, value, defaultValue);
            }
        }
        return defaultValue;
    }

    private static boolean getBooleanConfig(Map<String, Object> configs, String key, boolean defaultValue) {
        Object value = configs.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        if (value instanceof String s) {
            return Boolean.parseBoolean(s);
        }
        return defaultValue;
    }

    private record RefreshResult(int successCount, int failureCount, List<String> failedTopics) {
    }
}
