package com.statemigration.flink.externalstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.statemigration.flink.externalstore.rocksdb.EmbeddedRocksDbStore;
import com.statemigration.flink.externalstore.rocksdb.InMemoryMockRocksDbStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

public class ExternalizedStateJobTest {

    public static class CollectingSink implements SinkFunction<ExternalizedStateJob.OutputResult> {
        public static final List<ExternalizedStateJob.OutputResult> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(ExternalizedStateJob.OutputResult value, Context context) {
            VALUES.add(value);
        }
    }

    @Test
    public void testExternalizedStateWithMockedStoreAndSummation() throws Exception {
        // Arrange: Mock or spy the external key-value store (simulating an external RocksDB/Redis/Postgres)
        ExternalizedStateJob.ExternalKeyValueStore mockStore = mock(ExternalizedStateJob.ExternalKeyValueStore.class);
        ObjectMapper mapper = new ObjectMapper();

        // Simulate pre-existing legacy state stored externally (e.g. sum = 300.0 with an old "region" attribute)
        ExternalizedStateJob.DomainState legacyState = new ExternalizedStateJob.DomainState();
        legacyState.sum = 300.0;
        legacyState.attributes.put("region", "EU");
        byte[] serializedLegacyBytes = mapper.writeValueAsBytes(legacyState);

        when(mockStore.get("user_v1")).thenReturn(serializedLegacyBytes);

        // Instantiate function with the mocked external store
        ExternalizedStateJob.ExternalizedStateFlatMapFunction function =
                new ExternalizedStateJob.ExternalizedStateFlatMapFunction(mockStore);

        function.open(new Configuration());

        // Mock Flink Collector to capture output results
        List<ExternalizedStateJob.OutputResult> collectedOutputs = new ArrayList<>();
        Collector<ExternalizedStateJob.OutputResult> mockCollector = new Collector<ExternalizedStateJob.OutputResult>() {
            @Override
            public void collect(ExternalizedStateJob.OutputResult record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Send a new event with amount 50.0 and a brand-new evolved schema dimension ("tier")
        ExternalizedStateJob.UserInputEvent incomingEvent =
                new ExternalizedStateJob.UserInputEvent("user_v1", 50.0, "tier");

        function.flatMap(incomingEvent, mockCollector);

        // Assert: Verify numeric accumulation (300.0 + 50.0 = 350.0) and schema evolution
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_v1", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
        assertEquals("active_external", collectedOutputs.get(0).attributes.get("tier"));
        assertEquals("EU", collectedOutputs.get(0).attributes.get("region")); // Preserved legacy attribute

        // Verify external store was updated properly with the new schema state without touching Flink native binary savepoints
        verify(mockStore, times(1)).put(eq("user_v1"), any(byte[].class));
    }

    @Test
    public void testExternalizedStateWithInMemoryMockRocksDB() throws Exception {
        // Arrange: Instantiate our in-memory mock RocksDB store (zero disk, pure memory)
        InMemoryMockRocksDbStore mockRocksDbStore = new InMemoryMockRocksDbStore();
        ObjectMapper mapper = new ObjectMapper();

        // 1. Pre-seed legacy state directly into the in-memory store (sum = 300.0, region = "EU")
        ExternalizedStateJob.DomainState legacyState = new ExternalizedStateJob.DomainState();
        legacyState.sum = 300.0;
        legacyState.attributes.put("region", "EU");
        mockRocksDbStore.put("user_v1", mapper.writeValueAsBytes(legacyState));

        // 2. Instantiate the job flat map function with the in-memory mock store
        ExternalizedStateJob.ExternalizedStateFlatMapFunction function =
                new ExternalizedStateJob.ExternalizedStateFlatMapFunction(mockRocksDbStore);

        function.open(new Configuration());

        List<ExternalizedStateJob.OutputResult> collectedOutputs = new ArrayList<>();
        Collector<ExternalizedStateJob.OutputResult> mockCollector = new Collector<ExternalizedStateJob.OutputResult>() {
            @Override
            public void collect(ExternalizedStateJob.OutputResult record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Send an incoming event adding 50.0 and a new schema dimension ("tier")
        ExternalizedStateJob.UserInputEvent incomingEvent =
                new ExternalizedStateJob.UserInputEvent("user_v1", 50.0, "tier");

        function.flatMap(incomingEvent, mockCollector);

        // Assert: Verify numeric accumulation happened correctly (300.0 + 50.0 = 350.0)
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_v1", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
        assertEquals("active_external", collectedOutputs.get(0).attributes.get("tier"));
        assertEquals("EU", collectedOutputs.get(0).attributes.get("region")); // Preserved legacy attribute

        // 3. Verify data was correctly written back to the in-memory store structure
        byte[] persistedBytes = mockRocksDbStore.get("user_v1");
        ExternalizedStateJob.DomainState persistedState = mapper.readValue(persistedBytes, ExternalizedStateJob.DomainState.class);

        assertEquals(350.0, persistedState.sum, 0.001);
        assertEquals("active_external", persistedState.attributes.get("tier"));
    }

    /*
     * EmbeddedRocksDbStore is a simple wrapper around RocksDB to provide an external key-value store for Flink state.
     * It allows storing and retrieving byte arrays by string keys, simulating an external database.
     * This implementation uses RocksDB as the underlying storage engine.
     *
     * It depends on the RocksDB JNI library, which must be included in the project dependencies.
     * I opt out not to install it, as this is my job laptop, but feel free to install it if you want to run the REAL
     * RocksDB-based test below.
     */
//    @Test
//    public void testExternalizedStateWithRealEmbeddedRocksDB(@TempDir Path tempDir) throws Exception {
//        // Arrange: Spin up a REAL embedded RocksDB store pointing to a JUnit temporary folder
//        try (EmbeddedRocksDbStore realRocksDbStore = new EmbeddedRocksDbStore(tempDir.toAbsolutePath().toString())) {
//            ObjectMapper mapper = new ObjectMapper();
//
//            // 1. Seed a legacy state directly into the real RocksDB database (sum = 300.0, region = "EU")
//            ExternalizedStateJob.DomainState legacyState = new ExternalizedStateJob.DomainState();
//            legacyState.sum = 300.0;
//            legacyState.attributes.put("region", "EU");
//            realRocksDbStore.put("user_v1", mapper.writeValueAsBytes(legacyState));
//
//            // 2. Instantiate the job flat map function with the REAL RocksDB store
//            ExternalizedStateJob.ExternalizedStateFlatMapFunction function =
//                    new ExternalizedStateJob.ExternalizedStateFlatMapFunction(realRocksDbStore);
//
//            function.open(new Configuration());
//
//            List<ExternalizedStateJob.OutputResult> collectedOutputs = new ArrayList<>();
//            Collector<ExternalizedStateJob.OutputResult> mockCollector = new Collector<ExternalizedStateJob.OutputResult>() {
//                @Override
//                public void collect(ExternalizedStateJob.OutputResult record) {
//                    collectedOutputs.add(record);
//                }
//                @Override
//                public void close() {}
//            };
//
//            // Act: Send an incoming event adding 50.0 and a new schema dimension ("tier")
//            ExternalizedStateJob.UserInputEvent incomingEvent =
//                    new ExternalizedStateJob.UserInputEvent("user_v1", 50.0, "tier");
//
//            function.flatMap(incomingEvent, mockCollector);
//
//            // Assert: Verify state accumulation happened correctly via real database persistence (300.0 + 50.0 = 350.0)
//            assertEquals(1, collectedOutputs.size());
//            assertEquals("user_v1", collectedOutputs.get(0).key);
//            assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
//            assertEquals("active_external", collectedOutputs.get(0).attributes.get("tier"));
//            assertEquals("EU", collectedOutputs.get(0).attributes.get("region")); // Preserved legacy attribute
//
//            // 3. Verify data was physically written back to the real embedded RocksDB storage engine
//            byte[] persistedBytes = realRocksDbStore.get("user_v1");
//            ExternalizedStateJob.DomainState persistedState = mapper.readValue(persistedBytes, ExternalizedStateJob.DomainState.class);
//
//            assertEquals(350.0, persistedState.sum, 0.001);
//            assertEquals("active_external", persistedState.attributes.get("tier"));
//        }
//    }
}