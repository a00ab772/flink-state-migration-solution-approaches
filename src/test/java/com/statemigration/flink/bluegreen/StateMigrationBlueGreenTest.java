package com.statemigration.flink.bluegreen;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StateMigrationBlueGreenTest {

    public static class CollectingSink implements SinkFunction<StateMigrationBlueGreenJob.OutputResult> {
        public static final List<StateMigrationBlueGreenJob.OutputResult> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StateMigrationBlueGreenJob.OutputResult value, Context context) {
            VALUES.add(value);
        }
    }

    @Test
    public void testBlueGreenBootstrapAndParallelTransition() throws Exception {
        // Arrange: Initialize Green Pipeline function
        StateMigrationBlueGreenJob.GreenPipelineFunction greenFunction =
                new StateMigrationBlueGreenJob.GreenPipelineFunction();

        // Mock Flink runtime and state bootstrapping (Step C: Historical state bootstrapping / side-load)
        // Imagine Blue pipeline had accumulated 300.0. We bootstrap/transform that into Green's state format.
        org.apache.flink.api.common.functions.RuntimeContext mockRuntimeContext =
                mockRuntimeContextForStateBootstrap();

        greenFunction.setRuntimeContext(mockRuntimeContext);
        greenFunction.open(new Configuration());

        List<StateMigrationBlueGreenJob.OutputResult> collectedOutputs = new ArrayList<>();
        Collector<StateMigrationBlueGreenJob.OutputResult> mockCollector = new Collector<StateMigrationBlueGreenJob.OutputResult>() {
            @Override
            public void collect(StateMigrationBlueGreenJob.OutputResult record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Stream a new live event into the Green pipeline during the parallel transition window
        StateMigrationBlueGreenJob.UserEvent liveEvent =
                new StateMigrationBlueGreenJob.UserEvent("user_blue_green", 50.0, "GREEN_V2");

        greenFunction.flatMap(liveEvent, mockCollector);

        // Assert: Verify that bootstrapped historical state (300.0) correctly combined with live event (50.0) = 350.0
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_blue_green", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);
        assertEquals("GREEN_V2", collectedOutputs.get(0).pipelineVersion);
        assertEquals("dual_pipeline_blue_green", collectedOutputs.get(0).attributes.get("migrated_via"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private org.apache.flink.api.common.functions.RuntimeContext mockRuntimeContextForStateBootstrap() throws Exception {
        org.apache.flink.api.common.functions.RuntimeContext mockContext =
                org.mockito.Mockito.mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        org.apache.flink.api.common.state.ValueState<StateMigrationBlueGreenJob.GreenPipelineState> mockState =
                org.mockito.Mockito.mock(org.apache.flink.api.common.state.ValueState.class);

        // Pre-loaded bootstrapped state representing historical migration from Blue V1 (sum = 300.0)
        StateMigrationBlueGreenJob.GreenPipelineState bootstrappedState = new StateMigrationBlueGreenJob.GreenPipelineState();
        bootstrappedState.totalSum = 300.0;

        org.mockito.Mockito.when(mockState.value()).thenReturn(bootstrappedState);

        // Usar ValueStateDescriptor.class para coincidir exactamente con la firma del método de Flink
        org.mockito.Mockito.when(mockContext.getState(org.mockito.ArgumentMatchers.any(org.apache.flink.api.common.state.ValueStateDescriptor.class)))
                .thenReturn((org.apache.flink.api.common.state.ValueState) mockState);

        return mockContext;
    }
}