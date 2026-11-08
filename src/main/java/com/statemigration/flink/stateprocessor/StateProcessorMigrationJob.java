package com.statemigration.flink.stateprocessor;

import com.statemigration.flink.stateprocessor.schemaversions.V1AggregateState;
import com.statemigration.flink.stateprocessor.schemaversions.V2AggregateState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class StateProcessorMigrationJob {

    /**
     * Bootstrap function that converts legacy V1 state into V2 schema.
     */
    public static class V1ToV2StateBootstrapFunction extends KeyedStateBootstrapFunction<String, V1AggregateState> {

        private transient ValueState<V2AggregateState> v2State;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<V2AggregateState> descriptor = new ValueStateDescriptor<>(
                    "v2-domain-state",
                    V2AggregateState.class
            );
            v2State = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(V1AggregateState v1Input, Context context) throws Exception {
            // Upcast V1 total to V2 sum and attach default new field metadata
            V2AggregateState transformedState = new V2AggregateState();
            transformedState.sum = v1Input.total;
            transformedState.newConfigurableField = "migrated_via_state_processor_api";

            // Store in newly formatted V2 state backend
            v2State.update(transformedState);
        }
    }
}