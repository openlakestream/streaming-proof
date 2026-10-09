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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.ProofValue;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarAtLeastOnceProofConsumerTest {

    @DataProvider
    public Object[][] offloadWaitConfigs() {
        return new Object[][]{
                {Map.of(
                        "offload.condition.waitOffloadedForEachLedger", true,
                        "offload.condition.initialRetryBackoffMs", TimeUnit.SECONDS.toMillis(30),
                        "offload.condition.allowDegradedMode", false)},
                {Map.of(
                        "offload.condition.offloadedLedgersBeforeCatchupRead", 1,
                        "offload.condition.initialRetryBackoffMs", TimeUnit.SECONDS.toMillis(30),
                        "offload.condition.allowDegradedMode", false)}
        };
    }

    @Test(dataProvider = "offloadWaitConfigs", timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void shouldStopWithoutProcessingMessageWhenOffloadWaitIsInterrupted(
            Map<String, Object> configs) throws Exception {
        String topic = "wPG0v";
        String partition0 = OffloadCondition.partitionName(topic, 0);
        PulsarClient client = mock(PulsarClient.class);
        ConsumerBuilder<ProofValue> builder = mock(ConsumerBuilder.class, RETURNS_SELF);
        Consumer<ProofValue> pulsarConsumer = mock(Consumer.class);
        Message<ProofValue> message = mock(Message.class);
        MessageIdAdv messageId = mock(MessageIdAdv.class);
        MessageListener listener = mock(MessageListener.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        Topics topics = mock(Topics.class);

        when(client.newConsumer(any(ProofValueSchema.class))).thenReturn(builder);
        when(builder.subscribe()).thenReturn(pulsarConsumer);
        when(pulsarConsumer.receive()).thenReturn(message);
        when(message.getKey()).thenReturn("key");
        when(message.getValue()).thenReturn(ProofValue.of(1L));
        when(message.getMessageId()).thenReturn(messageId);
        when(messageId.getLedgerId()).thenReturn(123L);
        when(admin.topics()).thenReturn(topics);
        when(topics.getPartitionedTopicMetadata(topic)).thenReturn(new PartitionedTopicMetadata(1));
        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();
        stats.ledgers = java.util.List.of();
        when(topics.getInternalStats(partition0)).thenReturn(stats);

        PulsarAtLeastOnceProofConsumer consumer = new PulsarAtLeastOnceProofConsumer(
                "test-consumer", client, topic, configs, 0, listener, admin);

        try {
            verify(topics, org.mockito.Mockito.timeout(TimeUnit.SECONDS.toMillis(3)))
                    .getInternalStats(partition0);
        } finally {
            consumer.close();
        }

        verify(listener, never()).onMessage(anyString(), anyLong(), any());
        verify(pulsarConsumer, never()).acknowledge(any(Message.class));
        verify(pulsarConsumer, never()).negativeAcknowledge(any(Message.class));
        verify(pulsarConsumer).close();
    }
}
