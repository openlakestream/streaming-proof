# Webhook Notification Feature

## Overview

The streaming-proof framework now supports webhook notifications that are automatically sent when a proof test completes its configured duration. This feature allows you to receive real-time notifications about test completion in your preferred communication channels like Slack, Discord, or any custom webhook endpoint.

## Features

- **Automatic Duration Completion Notifications**: Receive notifications when a proof test reaches its configured duration
- **Multiple Webhook Types**: Support for Slack, Discord, and generic webhooks
- **Configurable**: Enable/disable notifications per proof test
- **Detailed Information**: Notifications include proof details, duration, start/completion times, and status

## Configuration

### Basic Usage

When creating a proof test, you can include webhook configuration in the request:

```json
{
  "name": "kafka-ordering-test",
  "driver": "kafka",
  "topic": "test-topic",
  "duration": 3600,
  "webhookConfig": {
    "url": "https://example.invalid/redacted-slack-webhook",
    "type": "slack",
    "enabled": true
  }
}
```

### Webhook Configuration Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | - | The webhook URL to send notifications to (required) |
| `type` | string | "slack" | The webhook type: "slack", "discord", or "generic" |
| `enabled` | boolean | true | Whether webhook notifications are enabled |

### Supported Webhook Types

#### Slack
```json
{
  "webhookConfig": {
    "url": "https://example.invalid/redacted-slack-webhook",
    "type": "slack"
  }
}
```

#### Discord
```json
{
  "webhookConfig": {
    "url": "https://discord.com/api/webhooks/YOUR/DISCORD/WEBHOOK",
    "type": "discord"
  }
}
```

#### Generic Webhook
```json
{
  "webhookConfig": {
    "url": "https://your-custom-endpoint.com/webhook",
    "type": "generic"
  }
}
```

## Notification Content

### Duration Completion Notification

When a proof test reaches its configured duration, a notification like this will be sent:

```
⏰ **Streaming Proof Test Duration Completed**

**Proof ID:** test-12345
**Name:** kafka-ordering-test
**Driver:** kafka
**Topic:** test-topic
**Duration:** 3600 seconds
**Start Time:** 2024-01-15T10:30:00
**Completion Time:** 2024-01-15T11:30:00

**Status:** ⏱️ Duration completed - test finished after configured duration
```

### Test Completion Notification

When a proof test completes with results, the notification uses the final `ProofReport`
status and result reason, including failures caused by the verification stall limit:

`Completion Time` is the report's recorded completion time, rather than the time the webhook is sent.

```
🎯 **Streaming Proof Test Completed**

**Proof ID:** test-12345
**Name:** kafka-ordering-test
**Driver:** kafka
**Topic:** test-topic
**Duration:** 3600 seconds
**Start Time:** 2024-01-15T10:30:00
**Completion Time:** 2024-01-15T11:30:00

Status: ✅ passed
Reason: The run completed without missed or out-of-order messages.
```

## API Examples

### Creating a Proof with Slack Webhook

```bash
curl -X POST http://localhost:8080/proof \
  -H "Content-Type: application/json" \
  -d '{
    "name": "kafka-performance-test",
    "driver": "kafka",
    "topic": "perf-test",
    "partitions": 8,
    "producers": 4,
    "consumers": 4,
    "msgRate": 5000,
    "duration": 7200,
    "webhookConfig": {
      "url": "https://example.invalid/redacted-slack-webhook",
      "type": "slack",
      "enabled": true
    }
  }'
```

### Creating a Proof with Discord Webhook

```bash
curl -X POST http://localhost:8080/proof \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pulsar-ordering-test",
    "driver": "pulsar",
    "topic": "order-test",
    "duration": 1800,
    "webhookConfig": {
      "url": "https://example.invalid/redacted-discord-webhook",
      "type": "discord"
    }
  }'
```

### Disabling Webhook Notifications

```bash
curl -X POST http://localhost:8080/proof \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-without-webhook",
    "driver": "kafka",
    "duration": 3600,
    "webhookConfig": {
      "enabled": false
    }
  }'
```

## Setting Up Webhooks

### Slack Setup

1. Go to your Slack workspace
2. Navigate to Apps → Incoming Webhooks
3. Create a new webhook for your desired channel
4. Copy the webhook URL
5. Use the URL in your proof configuration

### Discord Setup

1. Go to your Discord server
2. Navigate to Server Settings → Integrations → Webhooks
3. Create a new webhook for your desired channel
4. Copy the webhook URL
5. Use the URL in your proof configuration

## Error Handling

- If webhook delivery fails, the error will be logged but won't affect the proof test execution
- Completion notifications use the final report after verification finishes. Shutdown waits for
  delivery to complete or fail before closing the webhook client; this wait does not affect the test result.
- Invalid webhook configurations will be logged as warnings

## Implementation Details

### Classes Added/Modified

1. **`WebhookConfig`** - Configuration class for webhook settings
2. **`WebhookNotificationService`** - Service for sending webhook notifications
3. **`Proof`** - Added `webhookConfig` field
4. **`ProofTask`** - Added webhook notification logic for duration completion

### Dependencies

The webhook feature uses the existing AsyncHttpClient dependency, so no additional dependencies are required.

## Troubleshooting

### Common Issues

1. **Webhook not received**: Check if the webhook URL is correct and accessible
2. **Invalid webhook format**: Ensure the webhook type matches your endpoint (slack/discord/generic)
3. **Notifications disabled**: Verify that `enabled` is set to `true` in the webhook configuration

### Logs

Webhook-related logs can be found in the coordinator logs:

```
# Successful notification
INFO  - Successfully sent duration completion webhook notification for proof test-12345

# Failed notification
ERROR - Failed to send duration completion webhook notification for proof test-12345
```
