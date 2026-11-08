package com.statemigration.flink.upcasting;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class UpcastingJobTest {

    public static class CollectingSink implements SinkFunction<UpcastingJob.OutputResult> {
        public static final List<UpcastingJob.OutputResult> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UpcastingJob.OutputResult value, Context context) {
            VALUES.add(value);
        }
    }

    @Test
    public void testUpcastingStateFunctionExecution() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CollectingSink.VALUES.clear();

        env.fromElements(
                        new UpcastingJob.Event("user_1", 100.0),
                        new UpcastingJob.Event("user_2", 200.0),
                        new UpcastingJob.Event("user_1", 50.0)
                )
                .keyBy(UpcastingJob.Event::getKey)
                .process(new UpcastingJob.UpcastingStateFunction())
                .addSink(new CollectingSink());

        env.execute("Upcasting Job Test Execution");

        assertNotNull(CollectingSink.VALUES);
        assertEquals(3, CollectingSink.VALUES.size());

        double user1FinalSum = CollectingSink.VALUES.stream()
                .filter(r -> "user_1".equals(r.key))
                .mapToDouble(r -> r.totalSum)
                .reduce((first, second) -> second)
                .orElse(0.0);

        double user2FinalSum = CollectingSink.VALUES.stream()
                .filter(r -> "user_2".equals(r.key))
                .mapToDouble(r -> r.totalSum)
                .reduce((first, second) -> second)
                .orElse(0.0);

        assertEquals(150.0, user1FinalSum, 0.001);
        assertEquals(200.0, user2FinalSum, 0.001);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testUpcastingStateFunctionWithMockedV1State() throws Exception {
        // Arrange: Instantiate the Flink function
        UpcastingJob.UpcastingStateFunction function = new UpcastingJob.UpcastingStateFunction();

        // Mock the Flink execution context and ValueState
        org.apache.flink.api.common.functions.RuntimeContext mockRuntimeContext =
                mock(org.apache.flink.api.common.functions.RuntimeContext.class);
        ValueState<UpcastingJob.StateWrapper> mockValueState = mock(ValueState.class);

        // Simulate that stored state is actually Versión 1 with a total field value of 300.0
        UpcastingJob.V1AggregateState v1State = new UpcastingJob.V1AggregateState();
        v1State.total = 300.0;
        UpcastingJob.StateWrapper v1Wrapper = new UpcastingJob.StateWrapper(1, v1State);

        when(mockValueState.value()).thenReturn(v1Wrapper);
        when(mockRuntimeContext.getState(any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        // Inject RuntimeContext in the function
        function.setRuntimeContext(mockRuntimeContext);

        // Initialize the function using Flink's open() method
        function.open(new Configuration());

        // Mock Flink's Context and Collector in order to process the element
        KeyedProcessFunction.Context mockContext = mock(KeyedProcessFunction.Context.class);
        when(mockContext.getCurrentKey()).thenReturn("user_v1");

        List<UpcastingJob.OutputResult> collectedOutputs = new ArrayList<>();
        Collector<UpcastingJob.OutputResult> mockCollector = new Collector<UpcastingJob.OutputResult>() {
            @Override
            public void collect(UpcastingJob.OutputResult record) {
                collectedOutputs.add(record);
            }
            @Override
            public void close() {}
        };

        // Act: Send a new event that will trigger the V1 to V2 state upcasting
        UpcastingJob.Event incomingEvent = new UpcastingJob.Event("user_v1", 50.0);
        function.processElement(incomingEvent, mockContext, mockCollector);

        // Assert: Verify that state was successfully migrated to V2 and the sum  was correctly made (300.0 + 50.0 = 350.0)
        assertEquals(1, collectedOutputs.size());
        assertEquals("user_v1", collectedOutputs.get(0).key);
        assertEquals(350.0, collectedOutputs.get(0).totalSum, 0.001);

        // Verify the state was successfully updated with V2 in the backend, using the new field by default
        verify(mockValueState, times(1)).update(argThat(wrapper -> {
            UpcastingJob.StateWrapper w = (UpcastingJob.StateWrapper) wrapper;
            if (w.schemaVersion == 2 && w.payload instanceof UpcastingJob.V2AggregateState) {
                UpcastingJob.V2AggregateState v2 = (UpcastingJob.V2AggregateState) w.payload;
                return v2.sum == 350.0 && "migrated_default".equals(v2.newConfigurableField);
            }
            return false;
        }));
    }
}