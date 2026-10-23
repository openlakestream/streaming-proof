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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofReport;
import io.lakestream.streaming.proof.common.records.ProofSummary;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

/**
 * Service for sending webhook notifications when proof tests complete or timeout.
 * Supports different webhook types like Slack, Discord, etc.
 */
@Slf4j
public class WebhookNotificationService {
    
    private final AsyncHttpClient httpClient;
    private final ObjectMapper objectMapper;
    
    public WebhookNotificationService() {
        this.httpClient = Dsl.asyncHttpClient();
        this.objectMapper = new ObjectMapper();
    }
    
    public WebhookNotificationService(AsyncHttpClient httpClient) {
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * Sends a webhook notification for proof completion.
     * 
     * @param report The final proof report, including its result and correctness summary
     * @return CompletableFuture that completes when notification is sent
     */
    public CompletableFuture<Void> sendProofCompletionNotification(ProofReport report) {
        Proof proof = report.proof();
        if (proof.getWebhookConfig() == null || !proof.getWebhookConfig().isEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        
        try {
            String message = buildCompletionMessage(report);
            return sendWebhook(proof.getWebhookConfig(), message);
        } catch (Exception e) {
            log.error("Failed to send proof completion notification for proof {}", proof.getId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Sends a webhook notification for proof timeout.
     * 
     * @param proof The proof configuration
     * @return CompletableFuture that completes when notification is sent
     */
    public CompletableFuture<Void> sendProofTimeoutNotification(Proof proof) {
        if (proof.getWebhookConfig() == null || !proof.getWebhookConfig().isEnabled()) {
            return CompletableFuture.completedFuture(null);
        }
        
        try {
            String message = buildTimeoutMessage(proof);
            return sendWebhook(proof.getWebhookConfig(), message);
        } catch (Exception e) {
            log.error("Failed to send proof timeout notification for proof {}", proof.getId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Sends the actual webhook request.
     * 
     * @param config The webhook configuration
     * @param message The message to send
     * @return CompletableFuture that completes when webhook is sent
     */
    private CompletableFuture<Void> sendWebhook(WebhookConfig config, String message) {
        try {
            Map<String, Object> payload = createPayload(config.getType(), message);
            String jsonPayload = objectMapper.writeValueAsString(payload);
            
            Request request = new RequestBuilder("POST")
                    .setUrl(config.getUrl())
                    .setHeader("Content-Type", "application/json")
                    .setBody(jsonPayload)
                    .build();
            
            return httpClient.executeRequest(request)
                    .toCompletableFuture()
                    .thenApply(response -> {
                        if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
                            log.info("Successfully sent webhook notification to {}", config.getUrl());
                        } else {
                            log.warn("Webhook notification returned status code: {} for URL: {}", 
                                    response.getStatusCode(), config.getUrl());
                        }
                        return (Void) null;
                    })
                    .exceptionally(throwable -> {
                        log.error("Failed to send webhook notification to {}", config.getUrl(), throwable);
                        return (Void) null;
                    });
        } catch (Exception e) {
            log.error("Failed to prepare webhook request for {}", config.getUrl(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Creates the appropriate payload based on webhook type.
     * 
     * @param type The webhook type (slack, discord, etc.)
     * @param message The message content
     * @return The payload map
     */
    private Map<String, Object> createPayload(String type, String message) {
        Map<String, Object> payload = new HashMap<>();
        
        switch (type.toLowerCase()) {
            case "slack":
                List<Map<String, Object>> blocks = new ArrayList<>();
                Map<String, Object> section = new HashMap<>();
                section.put("type", "section");
                
                Map<String, Object> text = new HashMap<>();
                text.put("type", "mrkdwn");
                text.put("text", message);
                section.put("text", text);
                
                blocks.add(section);
                payload.put("blocks", blocks);
                break;
            case "discord":
                payload.put("content", message);
                break;
            default:
                // Generic webhook format
                payload.put("message", message);
                payload.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                break;
        }
        
        return payload;
    }
    
    /**
     * Builds a completion message for the proof.
     * 
     * @param report The final proof report
     * @return The formatted message
     */
    private String buildCompletionMessage(ProofReport report) {
        Proof proof = report.proof();
        ProofSummary summary = report.summary();
        StringBuilder message = new StringBuilder();
        message.append("Streaming Proof Test Completed\n\n");
        message.append("Proof ID: ").append(proof.getId()).append("\n");
        message.append("Name: ").append(proof.getName()).append("\n");
        message.append("Driver: ").append(proof.getDriver()).append("\n");
        message.append("Topic: ").append(proof.getTopic()).append("\n");
        message.append("Duration: ").append(proof.getDuration()).append(" seconds\n");
        message.append("Start Time: ").append(proof.getStartTime()).append("\n");
        if (report.completionTime() != null) {
            message.append("Completion Time: ").append(report.completionTime()).append("\n");
        }
        message.append("\n");
        
        String statusPrefix = switch (report.status()) {
            case "passed" -> "✅ ";
            case "failed" -> "❌ ";
            default -> "";
        };
        message.append("Status: ").append(statusPrefix).append(report.status()).append("\n");
        if (report.resultReason() != null) {
            message.append("Reason: ").append(report.resultReason()).append("\n");
        }
        if (summary != null) {
            message.append("Verified: ").append(summary.verified()).append("\n");
            if (summary.outOfOrders() > 0) {
                message.append("Out of Orders: ").append(summary.outOfOrders()).append("\n");
            }
            if (summary.missed() > 0) {
                message.append("Missed: ").append(summary.missed()).append("\n");
            }
        }
        
        return message.toString();
    }
    
    /**
     * Builds a timeout message for the proof.
     * 
     * @param proof The proof configuration
     * @return The formatted message
     */
    private String buildTimeoutMessage(Proof proof) {
        StringBuilder message = new StringBuilder();
        message.append("⏰ **Streaming Proof Test Duration Completed**\n\n");
        message.append("**Proof ID:** ").append(proof.getId()).append("\n");
        message.append("**Name:** ").append(proof.getName()).append("\n");
        message.append("**Driver:** ").append(proof.getDriver()).append("\n");
        message.append("**Topic:** ").append(proof.getTopic()).append("\n");
        message.append("**Duration:** ").append(proof.getDuration()).append(" seconds\n");
        message.append("**Start Time:** ").append(proof.getStartTime()).append("\n");
        message.append("**Completion Time:** ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .append("\n\n");
        message.append("**Status:** ⏱️ Duration completed - test finished after configured duration\n");
        
        return message.toString();
    }
    
    /**
     * Closes the HTTP client and releases resources.
     */
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (Exception e) {
            log.error("Failed to close HTTP client", e);
        }
    }
}
