package com.statemigration.flink.serialization.avro;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AvroStateSerializationTest {

    @Test
    public void testAvroStateSchemaEvolutionV1ToV2() throws Exception {
        // Arrange: Initialize Avro state flatmap operator
        AvroStateSerializationJob.AvroStateFlatMap flatMapFunction = new AvroStateSerializationJob.AvroStateFlatMap();

        // Simulate legacy V1 state restored from a savepoint (total=100.0, newConfigurableField=null)
        AvroAggregateState legacyV1State = new AvroAggregateState();
        legacyV1State.total = 100.0;
        legacyV1State.newConfigurableField = null; // Simulates missing field in V1 schema

        RuntimeContext mockRuntimeContext = mockRuntimeContextWithAvroState(legacyV1State);
        flatMapFunction.setRuntimeContext(mockRuntimeContext);
        flatMapFunction.open(new Configuration());

        List<AvroAggregateState> outputs = new ArrayList<>();
        Collector<AvroAggregateState> collector = new Collector<>() {
            @Override
            public void collect(AvroAggregateState record) {
                outputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Process a new event with value 50.0
        flatMapFunction.flatMap(50.0, collector);

        // Assert: Verify state accumulation (100.0 + 50.0 = 150.0) and field resolution
        assertEquals(1, outputs.size());
        assertEquals(150.0, outputs.get(0).total, 0.001);
        assertNotNull(outputs.get(0).newConfigurableField);
        assertEquals("evolved_via_avro_schema", outputs.get(0).newConfigurableField);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RuntimeContext mockRuntimeContextWithAvroState(AvroAggregateState initialAvroState) throws Exception {
        RuntimeContext mockContext = Mockito.mock(RuntimeContext.class);
        ValueState<AvroAggregateState> mockState = Mockito.mock(ValueState.class);

        Mockito.when(mockState.value()).thenReturn(initialAvroState);
        Mockito.when(mockContext.getState(ArgumentMatchers.any(ValueStateDescriptor.class)))
                .thenReturn((ValueState) mockState);

        return mockContext;
    }
}