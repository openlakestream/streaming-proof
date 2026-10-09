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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.testng.annotations.Test;

public class OffloadConditionTest {

    @Test
    public void shouldEnterDegradedModeWhenStatsRefreshNeverSucceeds() {
        TestTopicsState state = new TestTopicsState(1);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        state.failTopics.add(partition0);

        OffloadCondition condition = new OffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                true,
                0,
                40,
                1,
                5,
                true);

        condition.waitOffloadConditionMeetForMessage(createMessageId(123L));

        assertTrue(condition.isDegradedMode());
        assertTrue(state.getInternalStatsCalls >= 1);
    }

    @Test
    public void shouldNotWaitAgainForSameLedgerAfterEnteringDegradedMode() {
        TestTopicsState state = new TestTopicsState(1);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        state.failTopics.add(partition0);

        OffloadCondition condition = new OffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                true,
                0,
                40,
                1,
                5,
                true);

        condition.waitOffloadConditionMeetForMessage(createMessageId(123L));
        int firstCallCount = state.getInternalStatsCalls;

        condition.waitOffloadConditionMeetForMessage(createMessageId(123L));

        assertTrue(condition.isDegradedMode());
        assertEquals(state.getInternalStatsCalls, firstCallCount);
    }

    @Test
    public void shouldContinueWhenOnePartitionFailsAndTargetLedgerIsOffloaded() {
        TestTopicsState state = new TestTopicsState(2);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        String partition1 = OffloadCondition.partitionName("wPG0v", 1);
        state.statsByTopic.put(partition0, offloadedLedgerStats(123L));
        state.failTopics.add(partition1);

        OffloadCondition condition = new OffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                true,
                0,
                1000,
                1,
                5,
                true);

        condition.waitOffloadConditionMeetForMessage(createMessageId(123L));

        assertFalse(condition.isDegradedMode());
        assertEquals(state.unloadCallsByTopic.getOrDefault(partition0, 0), Integer.valueOf(1));
        assertEquals(state.unloadCallsByTopic.getOrDefault(partition1, 0), Integer.valueOf(1));
    }

    @Test
    public void shouldSkipPerLedgerWaitWhenOnlyCatchupConditionIsEnabled() {
        TestTopicsState state = new TestTopicsState(1);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        state.statsByTopic.put(partition0, offloadedLedgerStats(123L));

        OffloadCondition condition = OffloadCondition.getOffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                Map.of("offload.condition.offloadedLedgersBeforeCatchupRead", 1)).orElseThrow();

        condition.waitOffloadConditionMeetForMessage(createMessageId(123L));

        assertEquals(state.getInternalStatsCalls, 0);
        assertTrue(state.unloadCallsByTopic.isEmpty());
    }

    @Test
    public void shouldCacheOffloadedLedgersAndRequireNewLedgersForCatchupRead() {
        TestTopicsState state = new TestTopicsState(1);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        state.statsByTopic.put(partition0, offloadedLedgerStats(123L, 124L));

        OffloadCondition condition = new OffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                false,
                2,
                20,
                1,
                5,
                true);

        condition.waitOffloadConditionMeetForCatchupRead(123L);
        int statsCallsAfterFirstCheck = state.getInternalStatsCalls;

        state.statsByTopic.put(partition0, offloadedLedgerStats(123L, 124L, 125L));
        condition.waitOffloadConditionMeetForCatchupRead(124L);
        assertEquals(state.getInternalStatsCalls, statsCallsAfterFirstCheck);
        assertEquals(state.unloadCallsByTopic.getOrDefault(partition0, 0), Integer.valueOf(1));

        condition.waitOffloadConditionMeetForCatchupRead(125L);
        assertEquals(state.unloadCallsByTopic.getOrDefault(partition0, 0), Integer.valueOf(1));

        state.statsByTopic.put(partition0, offloadedLedgerStats(123L, 124L, 125L, 126L));
        condition.waitOffloadConditionMeetForCatchupRead(126L);

        assertEquals(state.unloadCallsByTopic.getOrDefault(partition0, 0), Integer.valueOf(2));
    }

    @Test(timeOut = 10000)
    public void shouldStopWaitingWhenInterruptedEvenIfDegradedModeIsDisabled() throws Exception {
        TestTopicsState state = new TestTopicsState(1);
        String partition0 = OffloadCondition.partitionName("wPG0v", 0);
        state.statsByTopic.put(partition0, offloadedLedgerStats());

        OffloadCondition condition = new OffloadCondition(
                createAdminProxy(createTopicsProxy(state)),
                "wPG0v",
                false,
                1,
                0,
                TimeUnit.SECONDS.toMillis(30),
                TimeUnit.SECONDS.toMillis(30),
                false);

        Thread waitThread = Thread.ofVirtual()
                .start(() -> condition.waitOffloadConditionMeetForCatchupRead(123L));
        try {
            assertTrue(state.internalStatsCalled.await(3, TimeUnit.SECONDS));

            state.statsByTopic.put(partition0, offloadedLedgerStats(123L));
            waitThread.interrupt();
            waitThread.join(TimeUnit.SECONDS.toMillis(3));

            assertFalse(waitThread.isAlive());
            assertTrue(waitThread.isInterrupted());
            assertEquals(state.getInternalStatsCalls, 1);
            assertTrue(state.unloadCallsByTopic.isEmpty());
        } finally {
            state.statsByTopic.put(partition0, offloadedLedgerStats(123L));
            waitThread.interrupt();
            waitThread.join(TimeUnit.SECONDS.toMillis(1));
        }
    }

    private PulsarAdmin createAdminProxy(Topics topics) {
        return (PulsarAdmin) Proxy.newProxyInstance(
                PulsarAdmin.class.getClassLoader(),
                new Class<?>[]{PulsarAdmin.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("topics".equals(name)) {
                        return topics;
                    }
                    if ("close".equals(name)) {
                        return null;
                    }
                    if ("toString".equals(name)) {
                        return "PulsarAdminTestProxy";
                    }
                    if ("hashCode".equals(name)) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(name)) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException("Unsupported PulsarAdmin method: " + name);
                });
    }

    private Topics createTopicsProxy(TestTopicsState state) {
        return (Topics) Proxy.newProxyInstance(
                Topics.class.getClassLoader(),
                new Class<?>[]{Topics.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getPartitionedTopicMetadata".equals(name)) {
                        return new PartitionedTopicMetadata(state.partitionCount);
                    }
                    if ("getInternalStats".equals(name)) {
                        String topic = (String) args[0];
                        PersistentTopicInternalStats stats = state.statsByTopic.get(topic);
                        state.getInternalStatsCalls++;
                        state.internalStatsCalled.countDown();
                        if (state.failTopics.contains(topic)) {
                            throw new RuntimeException("HTTP 503 no healthy upstream");
                        }
                        return stats;
                    }
                    if ("unload".equals(name)) {
                        String topic = (String) args[0];
                        state.unloadCallsByTopic.merge(topic, 1, Integer::sum);
                        return null;
                    }
                    if ("toString".equals(name)) {
                        return "TopicsTestProxy";
                    }
                    if ("hashCode".equals(name)) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(name)) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException("Unsupported Topics method: " + name);
                });
    }

    private MessageIdAdv createMessageId(long ledgerId) {
        return (MessageIdAdv) Proxy.newProxyInstance(
                MessageIdAdv.class.getClassLoader(),
                new Class<?>[]{MessageIdAdv.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("getLedgerId".equals(name)) {
                        return ledgerId;
                    }
                    if ("toString".equals(name)) {
                        return "MessageIdAdvTestProxy(" + ledgerId + ")";
                    }
                    if ("hashCode".equals(name)) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(name)) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException("Unsupported MessageIdAdv method: " + name);
                });
    }

    private PersistentTopicInternalStats offloadedLedgerStats(long... ledgerIds) {
        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();
        stats.ledgers = Arrays.stream(ledgerIds).mapToObj(ledgerId -> {
            ManagedLedgerInternalStats.LedgerInfo ledgerInfo = new ManagedLedgerInternalStats.LedgerInfo();
            ledgerInfo.ledgerId = ledgerId;
            ledgerInfo.offloaded = true;
            return ledgerInfo;
        }).toList();
        return stats;
    }

    private static class TestTopicsState {
        private final int partitionCount;
        private final Map<String, PersistentTopicInternalStats> statsByTopic = new HashMap<>();
        private final Set<String> failTopics = new HashSet<>();
        private final Map<String, Integer> unloadCallsByTopic = new HashMap<>();
        private final CountDownLatch internalStatsCalled = new CountDownLatch(1);
        private int getInternalStatsCalls;

        private TestTopicsState(int partitionCount) {
            this.partitionCount = partitionCount;
        }
    }
}
