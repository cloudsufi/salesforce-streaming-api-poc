package com.enterprise.salesforce.consumer;

import java.time.Instant;
import java.util.Map;

/**
 * Immutable envelope for a single event received from the Salesforce
 * Streaming API.  This is the object placed on the BlockingQueue and
 * consumed by the worker pool.
 *
 * <p>The raw CometD message payload looks like:
 * <pre>{@code
 * {
 *   "schema": "...",
 *   "payload": {
 *     "ChangeEventHeader": { "changeType": "UPDATE", ... },
 *     "Name": "Acme Corp"
 *   },
 *   "event": { "replayId": 12345 }
 * }
 * }</pre>
 */
public record StreamingEvent(
    /** CometD channel, e.g. /data/AccountChangeEvent or /event/MyEvent__e */
    String channel,

    /** Raw deserialized payload map (nested maps/lists for sub-objects). */
    Map<String, Object> payload,

    /** Monotonically increasing identifier used for replay. */
    long replayId,

    /** Wall-clock time the event was received by this process. */
    Instant receivedAt
) {

    /** Convenience: returns the ChangeEventHeader map for CDC events, or null. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> changeEventHeader() {
        Object p = payload.get("payload");
        if (p instanceof Map<?,?> payloadMap) {
            Object header = payloadMap.get("ChangeEventHeader");
            if (header instanceof Map<?,?> headerMap) {
                return (Map<String, Object>) headerMap;
            }
        }
        return null;
    }

    /** Returns the CDC change type (CREATE / UPDATE / DELETE / UNDELETE), or null. */
    public String changeType() {
        Map<String, Object> header = changeEventHeader();
        return header != null ? (String) header.get("changeType") : null;
    }

    /** Returns true if this is a CDC channel event. */
    public boolean isCdcEvent() {
        return channel.startsWith("/data/");
    }

    /** Returns true if this is a Platform Event. */
    public boolean isPlatformEvent() {
        return channel.startsWith("/event/");
    }
}
