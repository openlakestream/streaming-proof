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
package io.lakestream.streaming.proof.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * An immutable record that encapsulates message metadata for verification tracking.
 * 
 * <p>MessageMetadata captures system-specific positioning information that enables:
 * <ul>
 *   <li>Tracking message delivery order across distributed systems</li>
 *   <li>Comparing message positions to detect reordering</li>
 *   <li>Identifying message sources through partition information</li>
 *   <li>Supporting both Kafka-style offsets and Pulsar-style ledger/entry IDs</li>
 * </ul>
 *
 * <p>The metadata structure is designed to be flexible across different messaging systems:
 * <ul>
 *   <li>For Kafka: primarily uses offset and partition fields</li>
 *   <li>For Pulsar: primarily uses ledgerId and entryId fields</li>
 *   <li>For other systems: can be extended with additional fields as needed</li>
 * </ul>
 *
 * <p>The {@link #isAfter(MessageMetadata)} method provides a system-agnostic way to
 * compare message positions, which is essential for verifying ordering guarantees.
 *
 * @param offset The position of the message in a linear log (Kafka-style).
 *               A value of -1 indicates an invalid or uninitialized offset.
 * @param ledgerId The ID of the ledger containing the message (Pulsar-style).
 *                 A value of -1 indicates this field is not applicable.
 * @param entryId The position of the message within its ledger (Pulsar-style).
 *                A value of -1 indicates this field is not applicable.
 * @param partition The partition ID where the message was stored.
 *                  May be null if not applicable to the messaging system.
 * @param originalOffset The original offset of the message in the source topic,
 *                       this is for Kafka exactly-once verification.
 * @param publishTimestampMillis The publish timestamp reported by the messaging system,
 *                               when available. This is used for end-to-end latency.
 * @param batchIndex The index of the message within its Pulsar batch, or null when unbatched.
 * @param segmentId The Pulsar scalable-topic segment holding the message, or null for other
 *                  topics. A key only ever moves to segments with higher ids, so positions
 *                  in different segments are ordered by segment id first.
 *
 * @see ProofProducer#sendAsync(String, long)
 * @see MessageListener#onMessage(String, long, MessageMetadata)
 * @see io.lakestream.streaming.proof.common.records.ConsumerCheckPoint
 */
public record MessageMetadata(
        @JsonInclude(Include.NON_NULL)
        Long offset,
        
        @JsonInclude(Include.NON_NULL)
        Long ledgerId,
        
        @JsonInclude(Include.NON_NULL)
        Long entryId,

        @JsonInclude(Include.NON_NULL)
        Integer partition,

        @JsonInclude(Include.NON_NULL)
        Long originalOffset,

        @JsonInclude(Include.NON_NULL)
        Long publishTimestampMillis,

        @JsonInclude(Include.NON_NULL)
        Integer batchIndex,

        @JsonInclude(Include.NON_NULL)
        Long segmentId
) {

    public MessageMetadata(
            Long offset, Long ledgerId, Long entryId, Integer partition,
            Long originalOffset, Long publishTimestampMillis, Integer batchIndex) {
        this(offset, ledgerId, entryId, partition, originalOffset, publishTimestampMillis, batchIndex, null);
    }

    public MessageMetadata(Long offset, Long ledgerId, Long entryId, Integer partition, Long originalOffset) {
        this(offset, ledgerId, entryId, partition, originalOffset, -1L, null);
    }

    public MessageMetadata(
            Long offset, Long ledgerId, Long entryId, Integer partition,
            Long originalOffset, Long publishTimestampMillis) {
        this(offset, ledgerId, entryId, partition, originalOffset, publishTimestampMillis, null);
    }

    public static MessageMetadata empty() {
        return new MessageMetadata(-1L, -1L, -1L, null, -1L, -1L, null);
    }

    public MessageMetadata(long offset) {
        this(offset, null, null, null, -1L, -1L, null);
    }

    public MessageMetadata(long offset, int partition) {
        this(offset, null, null, partition, -1L, -1L, null);
    }

    public static MessageMetadata kafkaMetadata(long offset, int partition, Long originalOffset) {
        return new MessageMetadata(offset, null, null, partition, originalOffset, -1L, null);
    }

    public static MessageMetadata kafkaMetadata(
            long offset, int partition, Long originalOffset, Long publishTimestampMillis) {
        return new MessageMetadata(offset, null, null, partition, originalOffset, publishTimestampMillis, null);
    }

    public MessageMetadata(long ledgerId, long entryId) {
        this(null, ledgerId, entryId, null, -1L, -1L, null);
    }

    public MessageMetadata(long ledgerId, long entryId, int batchIndex) {
        this(null, ledgerId, entryId, null, -1L, -1L, batchIndex);
    }

    public MessageMetadata(long ledgerId, long entryId, long publishTimestampMillis) {
        this(null, ledgerId, entryId, null, -1L, publishTimestampMillis, null);
    }

    public boolean isAfter(MessageMetadata other) {
        if (other == null) {
            return true;
        }

        // If both have ledger and entry IDs, compare them
        if (this.ledgerId != null && this.entryId != null
                && other.ledgerId != null && other.entryId != null) {
            // Scalable topics: a key only ever moves on to segments with higher ids
            if (this.segmentId != null && other.segmentId != null && !this.segmentId.equals(other.segmentId)) {
                return this.segmentId > other.segmentId;
            }
            if (!this.ledgerId.equals(other.ledgerId)) {
                return this.ledgerId > other.ledgerId;
            }
            if (!this.entryId.equals(other.entryId)) {
                return this.entryId > other.entryId;
            }
            // Same ledger and entry — compare batch index for batched messages
            int thisBatch = this.batchIndex != null ? this.batchIndex : -1;
            int otherBatch = other.batchIndex != null ? other.batchIndex : -1;
            return thisBatch > otherBatch;
        }

        // Otherwise compare by offset
        return this.offset > other.offset;
    }

    @Override
    public String toString() {
        try {
            return Util.JSON_WRITER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
