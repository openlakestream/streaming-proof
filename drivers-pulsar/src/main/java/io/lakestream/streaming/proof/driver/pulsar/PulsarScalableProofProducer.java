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

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import io.lakestream.streaming.proof.common.ProofValue;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Produces proof messages to an Apache Pulsar 5 scalable topic ({@code topic://}) with the V5 client,
 * which routes every key to the segment currently owning its hash range and follows splits and merges.
 */
@Slf4j
public class PulsarScalableProofProducer implements ProofProducer {

    private final Producer<byte[]> producer;

    /** Total size in bytes of each message value, including the sequence number */
    private final int messageSize;

    public PulsarScalableProofProducer(PulsarClient client, String topic, int messageSize)
            throws PulsarClientException {
        this.messageSize = messageSize;
        this.producer = client.newProducer(Schema.bytes())
                .topic(topic)
                .sendTimeout(Duration.ZERO)
                .create();
        log.info("Created Pulsar scalable topic producer for topic: {}, message size: {} bytes", topic, messageSize);
    }

    @Override
    public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
        return producer.async().newMessage()
                .key(key)
                .value(new ProofValue(value, messageSize).encode())
                .send()
                .thenApply(id -> PulsarScalableMessageIds.toMetadata(id, -1L));
    }

    @Override
    public void close() throws Exception {
        producer.close();
        log.info("Closed Pulsar scalable topic producer for topic: {}", producer.topic());
    }
}
