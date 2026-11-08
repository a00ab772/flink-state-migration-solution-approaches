package com.statemigration.flink.schemaagnostic;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class GenericDslJob {

    public static class UserInputEvent {
        private String userId;
        private String metricKey;
        private double amount;
        private String configuredDimension; // E.g., newly evolved configuration fields
        public UserInputEvent() {}
        public UserInputEvent(String userId, String metricKey, double amount, String configuredDimension) {
            this.userId = userId;
            this.metricKey = metricKey;
            this.amount = amount;
            this.configuredDimension = configuredDimension;
        }
        public String getUserId() { return userId; }
        public String getMetricKey() { return metricKey; }
        public double getAmount() { return amount; }
        public String getConfiguredDimension() { return configuredDimension; }
    }

    public static class OutputResult {
        public String key;
        public double totalSum;
        public Map<String, Object> attributes;
    }

    // Flexible state container holding both numeric accumulation (sum) and dynamic attributes
    public static class DynamicAggregateState {
        public double sum = 0.0;
        public Map<String, Object> attributes = new HashMap<>();
        public DynamicAggregateState() {}
    }

    public static class GenericDslStateFunction extends KeyedProcessFunction<String, UserInputEvent, OutputResult> {
        private transient MapState<String, byte[]> genericState;
        private transient ObjectMapper objectMapper;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, byte[]> descriptor =
                    new MapStateDescriptor<>("generic-dsl-state", String.class, byte[].class);
            genericState = getRuntimeContext().getMapState(descriptor);
            objectMapper = new ObjectMapper();
        }

        @Override
        public void processElement(UserInputEvent event, Context ctx, Collector<OutputResult> out) throws Exception {
            byte[] rawBytes = genericState.get(event.getMetricKey());
            DynamicAggregateState state = rawBytes == null ?
                    new DynamicAggregateState() :
                    objectMapper.readValue(rawBytes, DynamicAggregateState.class);

            // 1. Accumulate numeric sum (similar to UpcastingJob)
            state.sum += event.getAmount();

            // 2. Dynamically apply configuration dimensions if present (schema evolution)
            if (event.getConfiguredDimension() != null) {
                state.attributes.put(event.getConfiguredDimension(), "active");
            }

            // Save back to Flink's agnostic MapState container
            genericState.put(event.getMetricKey(), objectMapper.writeValueAsBytes(state));

            OutputResult res = new OutputResult();
            res.key = event.getUserId();
            res.totalSum = state.sum;
            res.attributes = state.attributes;
            out.collect(res);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
                        new UserInputEvent("user_1", "metrics_a", 100.0, "region"),
                        new UserInputEvent("user_1", "metrics_a", 50.0, "tier")
                )
                .keyBy(UserInputEvent::getUserId)
                .process(new GenericDslStateFunction())
                .print();

        env.execute("Flink State-Agnostic DSL Job");
    }
}