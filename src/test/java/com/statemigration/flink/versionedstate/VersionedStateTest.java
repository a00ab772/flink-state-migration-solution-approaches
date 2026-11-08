package com.statemigration.flink.versionedstate;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VersionedStateTest {

    @Test
    public void testVersionedStateUpcastFromV1ToV2() throws Exception {
        // Arrange: Initialize the flatmap function handling versioned state
        VersionedStateFlatMap flatMapFunction = new VersionedStateFlatMap();

        // Mock Flink runtime context with a pre-loaded V1 legacy state payload
        RuntimeContext mockRuntimeContext =
                mockRuntimeContextWithV1State();

        flatMapFunction.setRuntimeContext(mockRuntimeContext);
        flatMapFunction.open(new Configuration());

        List<VersionedStateModel.V2AggregateState> collectedOutputs = new ArrayList<>();
        Collector<VersionedStateModel.V2AggregateState> mockCollector = new Collector<>() {
            @Override
            public void collect(VersionedStateModel.V2AggregateState record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Send a new V2 event with an amount of 50.0 to be added after upcasting
        VersionedStateModel.V2AggregateState liveEvent = new VersionedStateModel.V2AggregateState();
        liveEvent.sum = 50.0;

        flatMapFunction.flatMap(liveEvent, mockCollector);

        // Assert: Verify legacy V1 total (150.0) was correctly upcast and added to the new event (50.0) = 200.0
        assertEquals(1, collectedOutputs.size());
        assertEquals(200.0, collectedOutputs.get(0).sum, 0.001);
        assertEquals("upcast_from_v1", collectedOutputs.get(0).newConfigurableField);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RuntimeContext mockRuntimeContextWithV1State() throws Exception {
        RuntimeContext mockContext =
                Mockito.mock(RuntimeContext.class);
        ValueState<VersionedStateModel.StateWrapper<Object>> mockState =
                Mockito.mock(ValueState.class);

        // Create a V1 legacy state structure wrapped with schemaVersion = 1
        VersionedStateModel.V1AggregateState oldV1State = new VersionedStateModel.V1AggregateState();
        oldV1State.total = 150.0; // Historical accumulated value in old format

        VersionedStateModel.StateWrapper<Object> v1Wrapper =
                new VersionedStateModel.StateWrapper<>(1, oldV1State);

        Mockito.when(mockState.value()).thenReturn(v1Wrapper);
        Mockito.when(mockContext.getState(ArgumentMatchers.any(org.apache.flink.api.common.state.ValueStateDescriptor.class)))
                .thenReturn(mockState);

        return mockContext;
    }
}