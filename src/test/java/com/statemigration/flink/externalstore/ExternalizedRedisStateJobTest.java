package com.statemigration.flink.externalstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ExternalizedRedisStateJobTest {

    @Test
    public void testExternalizedRedisStateWithMockedClient() throws Exception {
        // Arrange: Mock the external Redis key-value store interface
        ExternalizedStateJob.ExternalKeyValueStore mockRedisStore =
                mock(ExternalizedStateJob.ExternalKeyValueStore.class);
        ObjectMapper mapper = new ObjectMapper();

        // 1. Pre-seed legacy state stored externally in Redis (sum = 300.0, region = "EU")
        ExternalizedStateJob.DomainState legacyState = new ExternalizedStateJob.DomainState();
        legacyState.sum = 300.0;
        legacyState.attributes.put("region", "EU");
        byte[] serializedLegacyBytes = mapper.writeValueAsBytes(legacyState);

        when(mockRedisStore.get("user_redis_v1")).thenReturn(serializedLegacyBytes);

        // 2. Instantiate the job flat map function with the Redis store adapter
        ExternalizedStateJob.ExternalizedStateFlatMapFunction function =
                new ExternalizedStateJob.ExternalizedStateFlatMapFunction(mockRedisStore);

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

        // Act: Send a new incoming event adding 50.0 and a new schema dimension ("tier")
        ExternalizedStateJob.UserInputEvent incomingEvent =
                new ExternalizedStateJob.UserInputEvent("user_redis_v1", 50.0, "tier");

        function.flatMap(incomingEvent, mockCollector);

        // Assert: Verify numeric accumulation (300.0 + 50.0 = 350.0) and schema evolution
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_redis_v1", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
        assertEquals("active_external", collectedOutputs.get(0).attributes.get("tier"));
        assertEquals("EU", collectedOutputs.get(0).attributes.get("region")); // Preserved legacy attribute

        // Verify Redis store received the updated byte payload without touching Flink native savepoints
        verify(mockRedisStore, times(1)).put(eq("user_redis_v1"), any(byte[].class));
    }
}