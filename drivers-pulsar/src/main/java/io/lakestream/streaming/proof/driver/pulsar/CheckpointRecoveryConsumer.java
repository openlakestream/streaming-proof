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

import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClientException;

/** Exercises externally stored checkpoints while retaining the proof's verification state. */
@Slf4j
final class CheckpointRecoveryConsumer implements AutoCloseable {
    private final Factory factory;
    private final int restartMessages;
    private CheckpointConsumer<byte[]> consumer;
    private CheckpointConsumer<byte[]> pendingClose;
    private byte[] savedCheckpoint;
    private int processed;
    private boolean closed;

    CheckpointRecoveryConsumer(Factory factory, int restartMessages) throws PulsarClientException {
        if (restartMessages <= 0) {
            throw new IllegalArgumentException("restartMessages must be positive");
        }
        this.factory = factory;
        this.restartMessages = restartMessages;
        this.consumer = factory.create(Checkpoint.earliest());
    }

    Message<byte[]> receive() throws PulsarClientException {
        if (closed) {
            throw new IllegalStateException("Checkpoint recovery consumer is closed");
        }
        if (processed >= restartMessages) {
            prepareRestart();
        }
        if (consumer == null) {
            if (pendingClose != null) {
                pendingClose.close();
                pendingClose = null;
            }
            Checkpoint position;
            try {
                // Deserialize on every retry; never fall back to earliest after a failed restore.
                position = Checkpoint.fromByteArray(savedCheckpoint);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize saved checkpoint", e);
            }
            consumer = factory.create(position);
            log.info("Restored checkpoint consumer from serialized checkpoint ({} bytes)", savedCheckpoint.length);
        }
        return consumer.receive();
    }

    void messageProcessed() {
        processed++;
    }

    private void prepareRestart() {
        byte[] nextCheckpoint = consumer.checkpoint().toByteArray();
        try {
            // Validate serialization before closing the only live reader.
            Checkpoint.fromByteArray(nextCheckpoint);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to round-trip captured checkpoint", e);
        }
        savedCheckpoint = nextCheckpoint.clone();
        pendingClose = consumer;
        consumer = null;
        processed = 0;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        } finally {
            if (pendingClose != null) {
                pendingClose.close();
                pendingClose = null;
            }
        }
    }

    @FunctionalInterface
    interface Factory {
        CheckpointConsumer<byte[]> create(Checkpoint checkpoint) throws PulsarClientException;
    }
}
