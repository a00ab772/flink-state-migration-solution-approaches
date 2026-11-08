package com.statemigration.flink.stateprocessor;

import com.statemigration.flink.stateprocessor.schemaversions.V1AggregateState;
import com.statemigration.flink.stateprocessor.schemaversions.V2AggregateState;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;

public class StateProcessorMigrationTest {

    @Test
    public void testV1ToV2StateBootstrapMigration() throws Exception {
        // Arrange: Instantiate the bootstrap function responsible for offline state conversion
        StateProcessorMigrationJob.V1ToV2StateBootstrapFunction bootstrapFunction =
                new StateProcessorMigrationJob.V1ToV2StateBootstrapFunction();

        // Mock Flink runtime context and target V2 state storage
        RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);
        @SuppressWarnings("unchecked")
        ValueState<V2AggregateState> mockV2State = Mockito.mock(ValueState.class);

        Mockito.when(mockRuntimeContext.getState(ArgumentMatchers.any(ValueStateDescriptor.class)))
                .thenReturn(mockV2State);

        bootstrapFunction.setRuntimeContext(mockRuntimeContext);
        bootstrapFunction.open(new Configuration());

        // Prepare legacy V1 data record
        V1AggregateState v1Record = new V1AggregateState(250.75);

        // Act: Execute the state transformation function
        bootstrapFunction.processElement(v1Record, null);

        // Assert: Capture the state update call and verify exact field mappings
        ArgumentCaptor<V2AggregateState> stateCaptor = ArgumentCaptor.forClass(V2AggregateState.class);
        verify(mockV2State).update(stateCaptor.capture());

        V2AggregateState migratedState = stateCaptor.getValue();
        assertNotNull(migratedState);
        assertEquals(250.75, migratedState.sum, 0.001);
        assertEquals("migrated_via_state_processor_api", migratedState.newConfigurableField);
    }
}