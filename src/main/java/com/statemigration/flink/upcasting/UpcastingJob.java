package com.statemigration.flink.upcasting;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UpcastingJob {

    public static class Event {
        private String key;
        private double amount;
        public Event() {}
        public Event(String key, double amount) {
            this.key = key;
            this.amount = amount;
        }
        public String getKey() {
            return key;
        }
        public double getAmount() {
            return amount;
        }
    }

    public static class OutputResult {
        public String key;
        public double totalSum;
    }

    public static class V1AggregateState {
        public double total;
    }

    public static class V2AggregateState {
        public double sum;
        public String newConfigurableField;
    }

    public static class StateWrapper<T> {
        public int schemaVersion;
        public T payload;
        public StateWrapper() {}
        public StateWrapper(int schemaVersion, T payload) {
            this.schemaVersion = schemaVersion;
            this.payload = payload;
        }
    }

    /**
     * Pure upcasting function: transforms V1 state wrapper into V2 on the fly.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static StateWrapper upcastStateIfNeeded(StateWrapper wrapper) {
        if (wrapper.schemaVersion == 1) {
            V1AggregateState oldState = (V1AggregateState) wrapper.payload;
            V2AggregateState newState = new V2AggregateState();
            newState.sum = oldState.total;
            newState.newConfigurableField = "migrated_default";

            wrapper.payload = newState;
            wrapper.schemaVersion = 2;
        }
        return wrapper;
    }

    public static class UpcastingStateFunction extends KeyedProcessFunction<String, Event, OutputResult> {
        private transient ValueState<StateWrapper> state;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<StateWrapper> descriptor =
                    new ValueStateDescriptor<>("versioned-state", StateWrapper.class);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<OutputResult> out) throws Exception {
            StateWrapper wrapper = state.value();

            if (wrapper == null) {
                V2AggregateState v2 = new V2AggregateState();
                v2.sum = 0.0;
                v2.newConfigurableField = "default_config";
                wrapper = new StateWrapper(2, v2);
            } else {
                // Apply version upcasting if schema is older
                wrapper = upcastStateIfNeeded(wrapper);
            }

            V2AggregateState currentState = (V2AggregateState) wrapper.payload;
            currentState.sum += value.getAmount();
            state.update(wrapper);

            OutputResult res = new OutputResult();
            res.key = value.getKey();
            res.totalSum = currentState.sum;
            out.collect(res);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                        new Event("user_1", 100.0),
                        new Event("user_1", 50.0)
                )
                .keyBy(Event::getKey)
                .process(new UpcastingStateFunction())
                .print();

        env.execute("Flink Upcasting State Migration Job");
    }
}