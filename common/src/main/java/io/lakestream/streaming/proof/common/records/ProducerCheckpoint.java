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

import io.lakestream.streaming.proof.common.LongSeq;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/**
 * Tracks and manages checkpoint information for a producer.
 * This class is responsible for:
 * - Maintaining sequence numbers for published messages by key
 * - Tracking error counts by key
 */
@Data
public class ProducerCheckpoint {

    /** Map storing the maximum acknowledged sequence for each producer key. */
    private final Map<String, LongSeq> published = new HashMap<>();

    /** Map storing aggregated error timing details */
    private final Map<String, ErrorOccurrence> errorDetails = new HashMap<>();

    /**
     * Adds or updates the sequence number for a published message key.
     *
     * @param key The message key
     * @param seq The sequence number
     */
    public void addPublished(String key, LongSeq seq) {
        published.put(key, seq);
    }

    /**
     * Adds or merges timing details for an error message.
     *
     * @param key The error message
     * @param details Aggregated timing details for the error
     */
    public void addErrorDetails(String key, ErrorOccurrence details) {
        errorDetails.compute(key, (ignored, existing) -> {
            if (existing == null) {
                return details == null
                        ? null
                        : new ErrorOccurrence(
                                details.getCount(),
                                details.getFirstSeenAtMillis(),
                                details.getLastSeenAtMillis());
            }
            existing.merge(details);
            return existing;
        });
    }

    /**
     * Merges another ProducerCheckpoint into this one.
     * Published sequences and error counts from the other checkpoint will be added to this one.
     *
     * @param checkpoint The checkpoint to merge into this one
     */
    public void merge(ProducerCheckpoint checkpoint) {
        this.published.putAll(checkpoint.published);
        checkpoint.errorDetails.forEach(this::addErrorDetails);
    }
}
