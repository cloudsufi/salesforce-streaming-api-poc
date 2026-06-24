package com.enterprise.salesforce.processing;

import com.enterprise.salesforce.consumer.StreamingEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Concrete event processor implementations.
 *
 * <p>In a real integration these processors would write to Kafka, a JDBC
 * data store, or an Oracle GoldenGate trail file. The POC implementations
 * log the parsed event content so you can verify end-to-end connectivity.
 */
public final class EventProcessors {

    private EventProcessors() {}

    // -----------------------------------------------------------------------
    // CDC Processor
    // -----------------------------------------------------------------------

    /**
     * Processes CDC (Change Data Capture) events from {@code /data/*} channels.
     *
     * <p>A CDC event payload contains:
     * <ul>
     *   <li>{@code ChangeEventHeader} – metadata (changeType, changedFields, recordIds, etc.)</li>
     *   <li>Changed field values – only the fields that changed are present</li>
     * </ul>
     */
    public static class CdcEventProcessor implements Consumer<StreamingEvent> {

        private static final Logger log = LoggerFactory.getLogger(CdcEventProcessor.class);
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void accept(StreamingEvent event) {
            if (!event.isCdcEvent()) {
                log.warn("CdcEventProcessor received non-CDC event on channel={}. Skipping.",
                    event.channel());
                return;
            }

            Map<String, Object> header = event.changeEventHeader();

            // ----------------------------------------------------------------
            // Parse ChangeEventHeader fields
            // ----------------------------------------------------------------
            String changeType   = safeGet(header, "changeType");
            String entityName   = safeGet(header, "entityName");
            String transactionKey = safeGet(header, "transactionKey");
            String commitNumber = safeGet(header, "commitNumber");
            String commitUser   = safeGet(header, "commitUser");

            @SuppressWarnings("unchecked")
            List<String> recordIds = (List<String>) header.getOrDefault("recordIds", List.of());

            @SuppressWarnings("unchecked")
            List<String> changedFields = (List<String>) header.getOrDefault("changedFields", List.of());

            log.info("=== CDC Event ===");
            log.info("  Channel       : {}", event.channel());
            log.info("  ReplayId      : {}", event.replayId());
            log.info("  ChangeType    : {}", changeType);
            log.info("  Entity        : {}", entityName);
            log.info("  RecordIds     : {}", recordIds);
            log.info("  ChangedFields : {}", changedFields);
            log.info("  TxKey         : {}", transactionKey);
            log.info("  CommitNumber  : {}", commitNumber);
            log.info("  CommitUser    : {}", commitUser);

            // ----------------------------------------------------------------
            // Extract changed field values from the top-level payload
            // ----------------------------------------------------------------
            @SuppressWarnings("unchecked")
            Map<String, Object> payloadBody =
                (Map<String, Object>) event.payload().getOrDefault("payload", Map.of());

            payloadBody.forEach((field, value) -> {
                if (!"ChangeEventHeader".equals(field)) {
                    log.info("  Field [{}] = {}", field, value);
                }
            });

            // ----------------------------------------------------------------
            // TODO: route to downstream sink
            // Example: kafkaProducer.send(toKafkaRecord(event));
            //          jdbcWriter.upsert(entityName, recordIds, payloadBody);
            // ----------------------------------------------------------------
        }

        private static String safeGet(Map<String, Object> map, String key) {
            return map != null ? String.valueOf(map.getOrDefault(key, "null")) : "null";
        }
    }

    // -----------------------------------------------------------------------
    // Platform Event Processor
    // -----------------------------------------------------------------------

    /**
     * Processes Platform Events from {@code /event/*} channels.
     *
     * <p>Platform Event payloads contain the custom fields defined on the
     * Platform Event object, plus standard envelope fields:
     * <ul>
     *   <li>{@code CreatedById} – Salesforce user or connected app that published</li>
     *   <li>{@code CreatedDate} – ISO-8601 publish timestamp</li>
     * </ul>
     */
    public static class PlatformEventProcessor implements Consumer<StreamingEvent> {

        private static final Logger log = LoggerFactory.getLogger(PlatformEventProcessor.class);

        @Override
        public void accept(StreamingEvent event) {
            if (!event.isPlatformEvent()) {
                log.warn("PlatformEventProcessor received non-platform event on channel={}. Skipping.",
                    event.channel());
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> payloadBody =
                (Map<String, Object>) event.payload().getOrDefault("payload", Map.of());

            log.info("=== Platform Event ===");
            log.info("  Channel    : {}", event.channel());
            log.info("  ReplayId   : {}", event.replayId());
            log.info("  ReceivedAt : {}", event.receivedAt());

            // Standard envelope fields
            log.info("  CreatedById  : {}", payloadBody.get("CreatedById"));
            log.info("  CreatedDate  : {}", payloadBody.get("CreatedDate"));

            // Custom fields (all remaining keys)
            payloadBody.forEach((field, value) -> {
                if (!"CreatedById".equals(field) && !"CreatedDate".equals(field)) {
                    log.info("  CustomField [{}] = {}", field, value);
                }
            });

            // ----------------------------------------------------------------
            // TODO: route to downstream sink
            // Example: processBusinessEvent(event.channel(), payloadBody);
            // ----------------------------------------------------------------
        }
    }

    // -----------------------------------------------------------------------
    // Composite (routing) Processor
    // -----------------------------------------------------------------------

    /**
     * Routes events to the appropriate processor based on channel prefix.
     * Register this as the single {@code eventProcessor} in
     * {@link com.enterprise.salesforce.consumer.StreamingApiSubscriptionManager}.
     */
    public static class RoutingEventProcessor implements Consumer<StreamingEvent> {

        private static final Logger log = LoggerFactory.getLogger(RoutingEventProcessor.class);

        private final Consumer<StreamingEvent> cdcProcessor    = new CdcEventProcessor();
        private final Consumer<StreamingEvent> platformProcessor = new PlatformEventProcessor();

        @Override
        public void accept(StreamingEvent event) {
            if (event.isCdcEvent()) {
                cdcProcessor.accept(event);
            } else if (event.isPlatformEvent()) {
                platformProcessor.accept(event);
            } else {
                log.warn("Unknown channel type: {}. Raw payload: {}", event.channel(), event.payload());
            }
        }
    }
}
