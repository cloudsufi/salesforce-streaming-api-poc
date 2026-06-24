package com.enterprise.salesforce.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable Replay ID checkpoint store.
 *
 * <h2>Why Replay IDs matter</h2>
 * Salesforce retains CDC and Platform Events on its event bus for 72 hours
 * (3 days). Each event carries a monotonically increasing {@code replayId}.
 * If the consumer reconnects after a network failure, it can supply the last
 * checkpointed replayId to receive all events published since that point,
 * achieving <em>at-least-once</em> delivery.
 *
 * <h2>Special replay sentinel values</h2>
 * <ul>
 *   <li>{@code -1} – replay from the tip (newest events only, no backfill)</li>
 *   <li>{@code -2} – replay ALL retained events (full 72-hour backfill)</li>
 *   <li>{@code N}  – replay from event N+1 onward</li>
 * </ul>
 *
 * <h2>Storage strategy</h2>
 * In-memory map is the primary store; each successful checkpoint write is
 * also flushed to a local file (one line per channel: {@code channel=replayId}).
 * For production, replace the file backend with a distributed store
 * (Redis, ZooKeeper, a JDBC table, etc.) so that multiple connector
 * instances share checkpoint state.
 */
public class ReplayIdCheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(ReplayIdCheckpointStore.class);

    /** Sentinel: subscribe from new events only (no replay). */
    public static final long REPLAY_FROM_TIP = -1L;

    /** Sentinel: replay all retained events (72-hour window). */
    public static final long REPLAY_ALL = -2L;

    private final Map<String, Long> checkpoints = new ConcurrentHashMap<>();
    private final Path checkpointFile;

    public ReplayIdCheckpointStore(Path checkpointFile) {
        this.checkpointFile = checkpointFile;
        loadFromDisk();
    }

    /**
     * Returns the replayId to use when subscribing to a channel.
     * Returns {@link #REPLAY_FROM_TIP} if no checkpoint exists for the channel.
     *
     * @param channel  CometD channel name, e.g. {@code /data/AccountChangeEvent}
     * @return persisted replayId or {@code -1}
     */
    public long getReplayId(String channel) {
        return checkpoints.getOrDefault(channel, REPLAY_FROM_TIP);
    }

    /**
     * Persists the replayId after a batch of events from {@code channel}
     * has been successfully handed off to the processing queue.
     *
     * <p><strong>Checkpoint-after-enqueue pattern:</strong> call this method
     * only <em>after</em> the event has been placed on the BlockingQueue,
     * never before. This guarantees that in the event of a crash the worker
     * will re-receive and re-process the event, preserving at-least-once
     * semantics.
     *
     * @param channel   CometD channel name
     * @param replayId  the replayId of the event just enqueued
     */
    public void checkpoint(String channel, long replayId) {
        Long previous = checkpoints.put(channel, replayId);
        if (previous == null || replayId > previous) {
            // Only flush to disk when the value actually advances (avoid
            // redundant I/O on duplicate deliveries).
            flushToDisk();
            log.debug("Checkpointed channel={} replayId={}", channel, replayId);
        }
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    private void loadFromDisk() {
        if (!Files.exists(checkpointFile)) {
            log.info("No checkpoint file found at {}. Starting from tip.", checkpointFile);
            return;
        }
        try {
            Files.readAllLines(checkpointFile).forEach(line -> {
                String[] parts = line.strip().split("=", 2);
                if (parts.length == 2) {
                    try {
                        checkpoints.put(parts[0], Long.parseLong(parts[1]));
                        log.info("Loaded checkpoint: channel={} replayId={}", parts[0], parts[1]);
                    } catch (NumberFormatException e) {
                        log.warn("Skipping malformed checkpoint line: {}", line);
                    }
                }
            });
        } catch (IOException e) {
            log.error("Failed to load checkpoints from disk. Starting from tip.", e);
        }
    }

    private synchronized void flushToDisk() {
        try {
            StringBuilder sb = new StringBuilder();
            checkpoints.forEach((channel, id) ->
                sb.append(channel).append('=').append(id).append('\n'));
            Files.writeString(checkpointFile, sb.toString(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            // Non-fatal: in-memory state is still correct. Log and continue.
            log.error("Failed to persist checkpoints to disk. "
                + "Replay may include already-processed events after restart.", e);
        }
    }
}
