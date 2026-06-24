package com.enterprise.salesforce;

import com.enterprise.salesforce.consumer.StreamingEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StreamingEventTest {

    @Test
    @DisplayName("isCdcEvent returns true for /data/ channels")
    void isCdcEvent() {
        StreamingEvent e = makeEvent("/data/AccountChangeEvent", Map.of());
        assertTrue(e.isCdcEvent());
        assertFalse(e.isPlatformEvent());
    }

    @Test
    @DisplayName("isPlatformEvent returns true for /event/ channels")
    void isPlatformEvent() {
        StreamingEvent e = makeEvent("/event/MyEvent__e", Map.of());
        assertTrue(e.isPlatformEvent());
        assertFalse(e.isCdcEvent());
    }

    @Test
    @DisplayName("changeType is extracted from ChangeEventHeader")
    void changeTypeExtracted() {
        Map<String, Object> header = new HashMap<>();
        header.put("changeType", "UPDATE");
        header.put("changedFields", List.of("Name", "Phone"));

        Map<String, Object> payloadBody = new HashMap<>();
        payloadBody.put("ChangeEventHeader", header);
        payloadBody.put("Name", "Acme Corp");

        Map<String, Object> raw = Map.of("payload", payloadBody);

        StreamingEvent e = makeEvent("/data/AccountChangeEvent", raw);
        assertEquals("UPDATE", e.changeType());
    }

    @Test
    @DisplayName("changeType returns null for Platform Events (no ChangeEventHeader)")
    void changeTypeNullForPlatformEvent() {
        StreamingEvent e = makeEvent("/event/MyEvent__e", Map.of("payload", Map.of("EventType__c", "ORDER_CREATED")));
        assertNull(e.changeType());
    }

    @Test
    @DisplayName("changeEventHeader returns null when payload is empty")
    void changeEventHeaderNullWhenEmpty() {
        StreamingEvent e = makeEvent("/data/AccountChangeEvent", Map.of());
        assertNull(e.changeEventHeader());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static StreamingEvent makeEvent(String channel, Map<String, Object> payload) {
        return new StreamingEvent(channel, payload, 12345L, Instant.now());
    }
}
