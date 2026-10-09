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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.v5.MessageId;

/**
 * Turns the opaque message ids of a Pulsar 5 scalable topic into {@link MessageMetadata}.
 *
 * <p>The V5 client exposes no accessor for where a message is stored, so this relies on the
 * serialized MessageIdV5 layout: the 8-byte id of the segment, followed by the length-prefixed
 * id of the message inside that segment.
 */
final class PulsarScalableMessageIds {

    private PulsarScalableMessageIds() {
    }

    static MessageMetadata toMetadata(MessageId id, long publishTimeMillis) {
        ByteBuffer buf = ByteBuffer.wrap(id.toByteArray());
        long segmentId = buf.getLong();
        byte[] positionInSegment = new byte[buf.getInt()];
        buf.get(positionInSegment);
        try {
            MessageIdAdv position =
                    (MessageIdAdv) org.apache.pulsar.client.api.MessageId.fromByteArray(positionInSegment);
            return new MessageMetadata(null, position.getLedgerId(), position.getEntryId(), null, -1L,
                    publishTimeMillis, position.getBatchIndex(), segmentId);
        } catch (IOException e) {
            throw new UncheckedIOException("Unexpected message id " + id, e);
        }
    }
}
