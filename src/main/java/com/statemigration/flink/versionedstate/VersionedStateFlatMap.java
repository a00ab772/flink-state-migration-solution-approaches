package com.statemigration.flink.versionedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class VersionedStateFlatMap extends RichFlatMapFunction<VersionedStateModel.V2AggregateState, VersionedStateModel.V2AggregateState> {

    private transient ValueState<VersionedStateModel.StateWrapper<Object>> versionedState;

    @Override
    public void open(Configuration parameters) {
        // Register state with the wrapper type descriptor
        versionedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("versioned-domain-state", (Class) VersionedStateModel.StateWrapper.class)
        );
    }

    @Override
    public void flatMap(VersionedStateModel.V2AggregateState event, Collector<VersionedStateModel.V2AggregateState> out) throws Exception {
        VersionedStateModel.StateWrapper<Object> wrapper = versionedState.value();

        VersionedStateModel.V2AggregateState currentState;

        if (wrapper == null) {
            // Initialize fresh V2 state if none exists
            currentState = new VersionedStateModel.V2AggregateState();
        } else if (wrapper.schemaVersion == 1) {
            // UPCASTING: Convert legacy V1 state payload to V2 dynamically
            VersionedStateModel.V1AggregateState oldV1 = (VersionedStateModel.V1AggregateState) wrapper.payload;
            currentState = new VersionedStateModel.V2AggregateState();
            currentState.sum = oldV1.total; // Map old 'total' to new 'sum'
            currentState.newConfigurableField = "upcast_from_v1";
        } else {
            // Already V2 schema
            currentState = (VersionedStateModel.V2AggregateState) wrapper.payload;
        }

        // Apply incoming event logic
        currentState.sum += event.sum;

        // Save back wrapped with current schema version (v2)
        versionedState.update(new VersionedStateModel.StateWrapper<>(2, currentState));

        out.collect(currentState);
    }
}