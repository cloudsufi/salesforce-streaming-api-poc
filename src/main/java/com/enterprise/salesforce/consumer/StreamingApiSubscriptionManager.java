package com.enterprise.salesforce.consumer;

import com.enterprise.salesforce.auth.SalesforceAuthManager;
import com.enterprise.salesforce.auth.SalesforceAuthManager.BearerToken;
import com.enterprise.salesforce.checkpoint.ReplayIdCheckpointStore;
import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Manages EMP Connector subscriptions to one or more Salesforce Streaming API
 * channels.
 *
 * <h2>Architecture</h2>
 * <pre>
 *   CometD Thread (EMP Connector callback)
 *       │  offers event to BlockingQueue (non-blocking – drops if full)
 *       ▼
 *   BlockingQueue&lt;StreamingEvent&gt;  (bounded, 10 000 events)
 *       │  dispatcher thread drains queue
 *       ▼
 *   ExecutorService (Worker Pool – fixed thread pool)
 *       │  each worker calls EventProcessor.process(event)
 *       ▼
 *   EventProcessor (downstream: DB write, Kafka publish, etc.)
 * </pre>
 *
 * <h2>Why separate the callback from processing?</h2>
 * CometD callbacks run on Jetty I/O threads. Blocking those threads with slow
 * downstream operations (DB writes, network calls) will stall the long-poll
 * cycle and cause the Salesforce server to time out the connection. The
 * BlockingQueue + worker pool pattern decouples reception from processing.
 */
public class StreamingApiSubscriptionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StreamingApiSubscriptionManager.class);

    private static final int QUEUE_CAPACITY    = 10_000;
    private static final int WORKER_THREAD_COUNT = 4;

    private final SalesforceAuthManager authManager;
    private final ReplayIdCheckpointStore checkpointStore;
    private final List<String> channels;
    private final Consumer<StreamingEvent> eventProcessor;

    private EmpConnector connector;
    private final BlockingQueue<StreamingEvent> eventQueue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final ExecutorService workerPool =
            Executors.newFixedThreadPool(WORKER_THREAD_COUNT, r -> {
                Thread t = new Thread(r, "sf-event-worker");
                t.setDaemon(true);
                return t;
            });
    private Thread dispatcherThread;
    private volatile boolean running = false;

    public StreamingApiSubscriptionManager(
            SalesforceAuthManager authManager,
            ReplayIdCheckpointStore checkpointStore,
            List<String> channels,
            Consumer<StreamingEvent> eventProcessor) {

        this.authManager    = authManager;
        this.checkpointStore = checkpointStore;
        this.channels       = List.copyOf(channels);
        this.eventProcessor = eventProcessor;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public synchronized void start() throws Exception {
        if (running) {
            log.warn("StreamingApiSubscriptionManager is already running.");
            return;
        }

        BearerToken token = authManager.getValidToken();
        log.info("Starting EMP Connector against {}", token.cometDUrl());

        BayeuxParameters params = buildBayeuxParameters(token);
        connector = new EmpConnector(params);

        // NOTE: EmpConnector 0.0.1-SNAPSHOT does not expose addErrorHandler().
        // Connector-level errors surface via the Future returned by start()
        // and through subscription failures, both of which are handled below.

        connector.start().get(30, TimeUnit.SECONDS);
        log.info("Bayeux handshake complete.");

        for (String channel : channels) {
            subscribeChannel(channel);
        }

        running = true;
        dispatcherThread = new Thread(this::dispatchLoop, "sf-event-dispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        log.info("StreamingApiSubscriptionManager started. Listening on {} channel(s).",
                channels.size());
    }

    @Override
    public synchronized void close() {
        if (!running) return;
        log.info("Shutting down StreamingApiSubscriptionManager...");
        running = false;

        if (connector != null) {
            connector.stop();
        }

        dispatcherThread.interrupt();

        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Worker pool did not terminate cleanly; forcing shutdown.");
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerPool.shutdownNow();
        }
        log.info("StreamingApiSubscriptionManager stopped.");
    }

    // -----------------------------------------------------------------------
    // Subscription
    // -----------------------------------------------------------------------

    private void subscribeChannel(String channel) throws Exception {
        long replayId = checkpointStore.getReplayId(channel);
        log.info("Subscribing to channel={} replayId={}", channel, replayId);

        Consumer<Map<String, Object>> empCallback = rawMessage -> {
            try {
                long msgReplayId = extractReplayId(rawMessage);
                StreamingEvent event = new StreamingEvent(
                        channel, rawMessage, msgReplayId, Instant.now());

                boolean enqueued = eventQueue.offer(event);
                if (!enqueued) {
                    log.warn("Event queue full! Dropping event replayId={} on channel={}. "
                            + "Increase QUEUE_CAPACITY or worker throughput.", msgReplayId, channel);
                } else {
                    checkpointStore.checkpoint(channel, msgReplayId);
                }
            } catch (Exception e) {
                log.error("Error in CometD callback for channel={}", channel, e);
            }
        };

        TopicSubscription subscription = connector
                .subscribe(channel, replayId, empCallback)
                .get(10, TimeUnit.SECONDS);

        log.info("Successfully subscribed: {}", subscription);
    }

    // -----------------------------------------------------------------------
    // Dispatcher
    // -----------------------------------------------------------------------

    private void dispatchLoop() {
        log.info("Event dispatcher thread started.");
        while (running || !eventQueue.isEmpty()) {
            try {
                StreamingEvent event = eventQueue.poll(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    workerPool.submit(() -> processEvent(event));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Dispatcher thread interrupted.");
                break;
            }
        }
        log.info("Event dispatcher thread stopped.");
    }

    private void processEvent(StreamingEvent event) {
        try {
            eventProcessor.accept(event);
        } catch (Exception e) {
            log.error("Error processing event replayId={} channel={}",
                    event.replayId(), event.channel(), e);
        }
    }

    // -----------------------------------------------------------------------
    // BayeuxParameters
    //
    // EmpConnector 0.0.1-SNAPSHOT defines BayeuxParameters as an interface
    // with only two abstract methods: bearerToken() and server().
    // The timeout/backoff methods were added in a later version and do not
    // exist in this snapshot, so they must NOT be annotated with @Override.
    // -----------------------------------------------------------------------

    private BayeuxParameters buildBayeuxParameters(BearerToken initialToken) {
        return new BayeuxParameters() {

            @Override
            public String bearerToken() {
                try {
                    return authManager.getValidToken().accessToken();
                } catch (Exception e) {
                    log.error("Failed to retrieve bearer token for reconnect", e);
                    return initialToken.accessToken();
                }
            }

            public URL server() {
                try {
                    return new URL(initialToken.cometDUrl());
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Invalid CometD URL: " + initialToken.cometDUrl(), e);
                }
            }

            // timeout(), backoffIncrement(), maxBackoff() existed in a later
            // version of BayeuxParameters. Removed here because 0.0.1-SNAPSHOT
            // does not declare them in the interface – adding @Override would
            // cause a compilation error.
        };
    }

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static long extractReplayId(Map<String, Object> rawMessage) {
        try {
            Map<String, Object> eventNode = (Map<String, Object>) rawMessage.get("event");
            if (eventNode != null) {
                Object id = eventNode.get("replayId");
                if (id instanceof Number n) return n.longValue();
            }
        } catch (Exception e) {
            log.warn("Could not extract replayId from message: {}", rawMessage, e);
        }
        return -1L;
    }
}