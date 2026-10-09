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

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.ProofConsumer;
import io.lakestream.streaming.proof.common.ProofValue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Consumes proof messages from an Apache Pulsar 5 scalable topic with one of the V5 consumer types:
 * <ul>
 *   <li>{@code stream} (default): per-key ordered delivery with cumulative acknowledgment, the broker
 *       assigning segments across the consumers of the subscription.</li>
 *   <li>{@code queue}: messages spread across the consumers without ordering and acknowledged
 *       individually, the counterpart of a Shared subscription.</li>
 *   <li>{@code checkpoint}: per-key ordered reads with no broker-managed position and no
 *       acknowledgment. The consumers form a group sharing the segments, each read from the start.</li>
 * </ul>
 */
@Slf4j
public class PulsarScalableProofConsumer implements ProofConsumer {

    /** Subscription of the stream and queue consumers, and group of the checkpoint consumers */
    private static final String SUBSCRIPTION = "streaming-proof";

    private final String name;
    private final MessageListener messageListener;
    private final long consumeDelayMs;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final Thread consumerThread;

    /** The underlying V5 consumer and how to receive and acknowledge with it */
    private final AutoCloseable consumer;
    private final Receiver receiver;
    private final Consumer<MessageId> acknowledger;
    private final Consumer<MessageId> negativeAcknowledger;

    public PulsarScalableProofConsumer(String name, PulsarClient client, String topic, String consumerType,
            long consumeDelayMs, MessageListener listener) throws PulsarClientException {
        this.name = name;
        this.messageListener = listener;
        this.consumeDelayMs = consumeDelayMs;
        switch (consumerType) {
            case "queue" -> {
                QueueConsumer<byte[]> queue = client.newQueueConsumer(Schema.bytes())
                        .topic(topic)
                        .subscriptionName(SUBSCRIPTION)
                        .consumerName(name)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                        .subscribe();
                this.consumer = queue;
                this.receiver = queue::receive;
                this.acknowledger = queue::acknowledge;
                this.negativeAcknowledger = queue::negativeAcknowledge;
            }
            case "checkpoint" -> {
                CheckpointConsumer<byte[]> checkpoint = client.newCheckpointConsumer(Schema.bytes())
                        .topic(topic)
                        .consumerGroup(SUBSCRIPTION)
                        .consumerName(name)
                        .startPosition(Checkpoint.earliest())
                        .create();
                this.consumer = checkpoint;
                this.receiver = checkpoint::receive;
                this.acknowledger = id -> { };
                this.negativeAcknowledger = id -> { };
            }
            default -> {
                StreamConsumer<byte[]> stream = client.newStreamConsumer(Schema.bytes())
                        .topic(topic)
                        .subscriptionName(SUBSCRIPTION)
                        .consumerName(name)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                        .subscribe();
                this.consumer = stream;
                this.receiver = stream::receive;
                // Also acknowledges everything delivered before it, on every segment
                this.acknowledger = stream::acknowledgeCumulative;
                this.negativeAcknowledger = id -> { };
            }
        }
        log.info("[{}] Created Pulsar scalable topic {} consumer for topic: {}", name, consumerType, topic);
        this.consumerThread = Thread.ofVirtual()
                .name("pulsar-scalable-proof-consumer-" + name)
                .start(this::consumeMessages);
    }

    private void consumeMessages() {
        while (!closing.get()) {
            Message<byte[]> message;
            try {
                message = receiver.receive();
            } catch (Exception e) {
                if (!closing.get()) {
                    log.error("[{}] Exception in consumer loop", name, e);
                }
                continue;
            }
            try {
                long publishTime = message.publishTime().toEpochMilli();
                long delayMs = publishTime + consumeDelayMs - System.currentTimeMillis();
                if (consumeDelayMs > 0 && delayMs > 0) {
                    Thread.sleep(delayMs);
                }
                messageListener.onMessage(message.key().orElse(null), ProofValue.decode(message.value()).seq(),
                        PulsarScalableMessageIds.toMetadata(message.id(), publishTime));
                acknowledger.accept(message.id());
            } catch (InterruptedException e) {
                return; // closing: the unprocessed message stays unacknowledged and is redelivered
            } catch (Exception e) {
                log.error("[{}] Error processing message {}", name, message.id(), e);
                negativeAcknowledger.accept(message.id());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (closing.compareAndSet(false, true)) {
            log.info("[{}] Closing Pulsar scalable topic consumer", name);
            consumerThread.interrupt();
            consumerThread.join();
            consumer.close();
            log.info("[{}] Pulsar scalable topic consumer closed", name);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @FunctionalInterface
    private interface Receiver {
        Message<byte[]> receive() throws PulsarClientException;
    }
}
