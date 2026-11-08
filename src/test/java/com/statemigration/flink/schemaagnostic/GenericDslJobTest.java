package com.statemigration.flink.schemaagnostic;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class GenericDslJobTest {

    public static class CollectingSink implements SinkFunction<GenericDslJob.OutputResult> {
        public static final List<GenericDslJob.OutputResult> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(GenericDslJob.OutputResult value, Context context) {
            VALUES.add(value);
        }
    }

    @Test
    public void testGenericDslWithPreSeededLegacyStateAndSummation() throws Exception {
        // Arrange: Instantiate the Flink function
        GenericDslJob.GenericDslStateFunction function = new GenericDslJob.GenericDslStateFunction();

        // Mock Flink's runtime context and MapState
        RuntimeContext mockRuntimeContext =
                mock(RuntimeContext.class);
        MapState<String, byte[]> mockMapState = mock(MapState.class);

        // Simulate pre-existing legacy state containing an accumulated sum of 300.0 and an old attribute
        ObjectMapper mapper = new ObjectMapper();
        GenericDslJob.DynamicAggregateState legacyState = new GenericDslJob.DynamicAggregateState();
        legacyState.sum = 300.0; // Legacy accumulated sum
        legacyState.attributes.put("region", "EU");
        byte[] serializedLegacyBytes = mapper.writeValueAsBytes(legacyState);

        when(mockMapState.get("metrics_a")).thenReturn(serializedLegacyBytes);
        when(mockRuntimeContext.getMapState(any(MapStateDescriptor.class))).thenReturn(mockMapState);

        // Inject RuntimeContext using Flink's standard method
        function.setRuntimeContext(mockRuntimeContext);
        function.open(new Configuration());

        // Mock Flink's Context and Collector
        KeyedProcessFunction.Context mockContext = mock(KeyedProcessFunction.Context.class);
        when(mockContext.getCurrentKey()).thenReturn("user_v1");

        List<GenericDslJob.OutputResult> collectedOutputs = new ArrayList<>();
        Collector<GenericDslJob.OutputResult> mockCollector = new Collector<GenericDslJob.OutputResult>() {
            @Override
            public void collect(GenericDslJob.OutputResult record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Send a new event with amount 50.0 and a brand-new evolved dimension ("tier")
        GenericDslJob.UserInputEvent incomingEvent =
                new GenericDslJob.UserInputEvent("user_v1", "metrics_a", 50.0, "tier");

        function.processElement(incomingEvent, mockContext, mockCollector);

        // Assert: Verify that sum correctly accumulated (300.0 + 50.0 = 350.0) and new attribute is present
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_v1", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
        assertEquals("active", collectedOutputs.get(0).attributes.get("tier"));
        assertEquals("EU", collectedOutputs.get(0).attributes.get("region")); // Preserved legacy attribute

        // Verify Flink's binary state container (`MapState<String, byte[]>`) remained completely agnostic and unchanged
        verify(mockMapState, times(1)).put(eq("metrics_a"), any(byte[].class));
    }
}