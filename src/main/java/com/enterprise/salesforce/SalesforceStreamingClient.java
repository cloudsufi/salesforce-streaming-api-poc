package com.enterprise.salesforce;

import com.enterprise.salesforce.auth.SalesforceAuthManager;
import com.enterprise.salesforce.auth.SalesforceAuthManager.BearerToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.TopicSubscription;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Test harness for Salesforce Streaming API via the EMP Connector (CometD).
 *
 * <h2>What this does</h2>
 * <ol>
 *   <li>Authenticates with Salesforce using {@link SalesforceAuthManager}
 *       (OAuth username-password flow).</li>
 *   <li>Performs the CometD Bayeux handshake and subscribes to the configured
 *       channel via {@link EmpConnector}.</li>
 *   <li>Every incoming event is serialized to JSON and written into a
 *       memory-mapped MapDB file ({@code ./logs/test-events.mapdb}).</li>
 *   <li>A dedicated reader thread polls the map every second, logs whatever
 *       it finds, and removes those entries.</li>
 *   <li>The main thread blocks on {@code readerThread.join()} so the process
 *       stays alive until Ctrl-C / SIGTERM.</li>
 * </ol>
 *
 * <h2>Running</h2>
 * <pre>
 * mvn package
 * java -jar target/streaming-api-poc-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
 *   -Dsf.loginUrl=https://login.salesforce.com \
 *   -Dsf.clientId=YOUR_CLIENT_ID \
 *   -Dsf.clientSecret=YOUR_CLIENT_SECRET \
 *   -Dsf.username=your@user.com \
 *   -Dsf.password=yourPasswordPlusToken \
 *   -Dsf.channel=/data/AccountChangeEvent
 * </pre>
 */
public class SalesforceStreamingClient {

    private static final Logger log = LoggerFactory.getLogger(SalesforceStreamingClient.class);

    /** Replay from the tip of the stream (-1 = latest event only). */
    private static final long REPLAY_FROM = EmpConnector.REPLAY_FROM_TIP;

    // Shared state --------------------------------------------------------

    private static final AtomicLong  eventCounter = new AtomicLong(0);
    private static final ObjectMapper MAPPER      = new ObjectMapper();

    /** Written by onEvent (CometD thread), read/removed by the reader thread. */
    private static volatile ConcurrentMap<Long, String> eventMap;

    /** MapDB handle – committed after each write, closed by JVM shutdown hook. */
    private static volatile DB mapDb;

    // ---------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        log.info("Salesforce Streaming API test harness starting...");
        // Load local.properties into system properties if it exists
        java.io.File localProps = new java.io.File(
                "C:\\Users\\shivang.khandelwal_c\\IdeaProjects\\streaming-api-poc\\streaming-api-poc\\local.properties");
        if (localProps.exists()) {
            java.util.Properties props = new java.util.Properties();
            try (java.io.FileInputStream fis = new java.io.FileInputStream(localProps)) {
                props.load(fis);
                props.forEach((k, v) -> System.setProperty((String) k, (String) v));
                log.info("Loaded local.properties");
            }
        }

        // ------------------------------------------------------------------
        // 1. Configuration
        // ------------------------------------------------------------------
        String loginUrl     = requireConfig("sf.loginUrl",     "SF_LOGIN_URL",
                "https://login.salesforce.com");
        String clientId     = requireConfig("sf.clientId",     "SF_CLIENT_ID",     null);
        String clientSecret = requireConfig("sf.clientSecret", "SF_CLIENT_SECRET", null);
        String username     = requireConfig("sf.username",     "SF_USERNAME",      null);
        String password     = requireConfig("sf.password",     "SF_PASSWORD",      null);
        String channel      = requireConfig("sf.channel",      "SF_CHANNEL",
                "/data/AccountChangeEvent");

        // ------------------------------------------------------------------
        // 2. MapDB setup
        // ------------------------------------------------------------------
        Files.createDirectories(Paths.get("./logs"));

        mapDb = DBMaker.fileDB("./logs/test-events.mapdb")
                .fileMmapEnableIfSupported()
                .transactionEnable()
                .closeOnJvmShutdown()
                .make();

        eventMap = mapDb.hashMap("events", Serializer.LONG, Serializer.STRING)
                .createOrOpen();

        log.info("MapDB opened at ./logs/test-events.mapdb");

        // ------------------------------------------------------------------
        // 3. Reader thread – polls the map every second
        // ------------------------------------------------------------------
        Thread readerThread = new Thread(() -> {
            log.info("Reader thread started.");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("Reader thread interrupted – exiting.");
                    break;
                }

                if (eventMap.isEmpty()) {
                    log.info("no events");
                } else {
                    for (Long key : eventMap.keySet().toArray(new Long[0])) {
                        String json = eventMap.remove(key);
                        if (json != null) {
                            log.info("Event [{}]: {}", key, json);
                        }
                    }
                    mapDb.commit();
                }
            }
            log.info("Reader thread stopped.");
        }, "event-reader");

        readerThread.setDaemon(false);
        readerThread.start();

        // ------------------------------------------------------------------
        // 4. Authenticate – getValidToken() performs the OAuth password flow
        //    and returns a BearerToken containing the access token +
        //    instance URL needed to build the CometD endpoint.
        // ------------------------------------------------------------------
        SalesforceAuthManager authManager = new SalesforceAuthManager(
                loginUrl, clientId, clientSecret, username, password);

        BearerToken token = authManager.getValidToken();
        log.info("Authenticated. Instance URL: {}", token.instanceUrl());

        // ------------------------------------------------------------------
        // 5. Build BayeuxParameters from the live token.
        //    bearerToken() is called by EmpConnector on every reconnect so
        //    it re-fetches a fresh token each time via getValidToken().
        // ------------------------------------------------------------------
        BayeuxParameters bayeuxParams = new BayeuxParameters() {

            @Override
            public String bearerToken() {
                try {
                    return authManager.getValidToken().accessToken();
                } catch (Exception e) {
                    log.error("Failed to refresh bearer token for reconnect", e);
                    return token.accessToken(); // fall back to original token
                }
            }

            // FIX: Changed 'server()' to '@Override public URL endpoint()'
            @Override
            public URL endpoint() {
                try {
                    // CRITICAL: Ensure token.cometDUrl() points to your Instance URL,
                    // NOT login.salesforce.com or test.salesforce.com
                    return new URL(token.cometDUrl());
                } catch (Exception e) {
                    throw new RuntimeException("Invalid CometD URL: " + token.cometDUrl(), e);
                }
            }
        };

        // ------------------------------------------------------------------
        // 6. CometD / EMP Connector – handshake then subscribe
        // ------------------------------------------------------------------
        Consumer<Map<String, Object>> eventConsumer = SalesforceStreamingClient::onEvent;

        EmpConnector connector = new EmpConnector(bayeuxParams);
        connector.start().get();
        log.info("CometD handshake complete. Subscribing to channel: {}", channel);

        TopicSubscription subscription = connector
                .subscribe(channel, REPLAY_FROM, eventConsumer)
                .get();
        log.info("Subscribed: {}", subscription);

        // ------------------------------------------------------------------
        // 7. JVM shutdown hook – stops connector and interrupts reader thread
        //    so that readerThread.join() unblocks and main() can return.
        // ------------------------------------------------------------------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received. Stopping connector and reader thread...");
            try {
                subscription.cancel();
                connector.stop();
            } catch (Exception e) {
                log.warn("Error stopping EmpConnector during shutdown: {}", e.getMessage());
            }
            readerThread.interrupt();
        }, "shutdown-hook"));

        // ------------------------------------------------------------------
        // 8. Block main thread until the reader thread exits
        // ------------------------------------------------------------------
        log.info("Test harness running. Press Ctrl-C to stop.");
        readerThread.join();

        log.info("Salesforce Streaming Client stopped cleanly.");
    }

    // ------------------------------------------------------------------
    // CometD event callback
    // ------------------------------------------------------------------

    /**
     * Invoked on the CometD callback thread for every inbound message.
     * Keeps work minimal: serialize to JSON, write to MapDB, commit.
     *
     * @param event raw event payload delivered by EMP Connector
     */
    private static void onEvent(Map<String, Object> event) {
        try {
            String json = MAPPER.writeValueAsString(event);
            long   key  = eventCounter.incrementAndGet();
            eventMap.put(key, json);
            mapDb.commit();
            log.debug("Event #{} written to MapDB.", key);
        } catch (Exception e) {
            log.error("Failed to persist event to MapDB.", e);
        }
    }

    // ------------------------------------------------------------------
    // Configuration helper
    // ------------------------------------------------------------------

    /**
     * Resolves a config value from (in priority order):
     * <ol>
     *   <li>Java system property ({@code -DsysProp=value})</li>
     *   <li>Environment variable</li>
     *   <li>The provided default</li>
     * </ol>
     *
     * @throws IllegalStateException if no value is found and defaultValue is null
     */
    private static String requireConfig(String sysProp, String envVar, String defaultValue) {
        String value = System.getProperty(sysProp);
        if (value == null || value.isBlank()) {
            value = System.getenv(envVar);
        }
        if (value == null || value.isBlank()) {
            if (defaultValue == null) {
                throw new IllegalStateException(
                        "Required configuration missing. Set -D" + sysProp
                                + " or environment variable " + envVar);
            }
            return defaultValue;
        }
        return value;
    }
}