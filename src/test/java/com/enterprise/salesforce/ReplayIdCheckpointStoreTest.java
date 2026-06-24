package com.enterprise.salesforce;

import com.enterprise.salesforce.checkpoint.ReplayIdCheckpointStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ReplayIdCheckpointStoreTest {

    @TempDir
    Path tempDir;

    private Path checkpointFile;
    private ReplayIdCheckpointStore store;

    @BeforeEach
    void setUp() {
        checkpointFile = tempDir.resolve("checkpoints.properties");
        store = new ReplayIdCheckpointStore(checkpointFile);
    }

    @Test
    @DisplayName("Returns REPLAY_FROM_TIP (-1) when no checkpoint exists for channel")
    void returnsDefaultReplayIdForUnknownChannel() {
        long id = store.getReplayId("/data/AccountChangeEvent");
        assertEquals(ReplayIdCheckpointStore.REPLAY_FROM_TIP, id);
    }

    @Test
    @DisplayName("Persists and retrieves a checkpoint correctly")
    void persistsAndRetrievesCheckpoint() {
        store.checkpoint("/data/AccountChangeEvent", 12345L);
        assertEquals(12345L, store.getReplayId("/data/AccountChangeEvent"));
    }

    @Test
    @DisplayName("Checkpoint advances monotonically – lower value does not overwrite higher")
    void checkpointIsMonotonicallyAdvancing() {
        store.checkpoint("/data/AccountChangeEvent", 500L);
        store.checkpoint("/data/AccountChangeEvent", 300L); // older – should not regress
        // In-memory value should still be 500 (the put still happens but disk write is skipped)
        // Verify that re-loading from disk gives the correct high-water mark
        ReplayIdCheckpointStore reloaded = new ReplayIdCheckpointStore(checkpointFile);
        // The last written value to disk is 500 (300 did not trigger a flush)
        assertEquals(500L, reloaded.getReplayId("/data/AccountChangeEvent"));
    }

    @Test
    @DisplayName("Survives a restart – checkpoint loaded from disk")
    void survivesRestart() {
        store.checkpoint("/data/AccountChangeEvent", 99999L);
        store.checkpoint("/event/MyEvent__e", 42L);

        // Simulate restart by creating a new store instance pointing at the same file
        ReplayIdCheckpointStore reloaded = new ReplayIdCheckpointStore(checkpointFile);
        assertEquals(99999L, reloaded.getReplayId("/data/AccountChangeEvent"));
        assertEquals(42L,    reloaded.getReplayId("/event/MyEvent__e"));
    }

    @Test
    @DisplayName("Multiple channels are independent")
    void multipleChannelsAreIndependent() {
        store.checkpoint("/data/AccountChangeEvent", 100L);
        store.checkpoint("/event/MyEvent__e",         200L);

        assertEquals(100L, store.getReplayId("/data/AccountChangeEvent"));
        assertEquals(200L, store.getReplayId("/event/MyEvent__e"));
        assertEquals(ReplayIdCheckpointStore.REPLAY_FROM_TIP,
                     store.getReplayId("/data/ContactChangeEvent")); // never checkpointed
    }

    @Test
    @DisplayName("REPLAY_ALL sentinel value is -2")
    void replayAllSentinelIsMinusTwo() {
        assertEquals(-2L, ReplayIdCheckpointStore.REPLAY_ALL);
    }
}
