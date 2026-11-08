package com.statemigration.flink.bluegreen;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class StateMigrationBlueGreenKafkaJob {

    public static class UserInputEvent {
        public String key;
        public double amount;
        public String dynamicDimension;
        public UserInputEvent() {}
    }

    public static class DomainState {
        public double sum = 0.0;
        public Map<String, Object> attributes = new HashMap<>();
        public DomainState() {}
    }

    public static class OutputResult {
        public String key;
        public double totalSum;
        public String pipelineVersion;
        public Map<String, Object> attributes = new HashMap<>();
        public OutputResult() {}
    }

    public static class BlueGreenKafkaFlatMap extends RichFlatMapFunction<String, OutputResult> {
        private transient ValueState<DomainState> state;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("green-domain-state", DomainState.class));
            mapper = new ObjectMapper();
        }

        @Override
        public void flatMap(String jsonValue, Collector<OutputResult> out) throws Exception {
            UserInputEvent event = mapper.readValue(jsonValue, UserInputEvent.class);

            DomainState currentState = state.value();
            if (currentState == null) {
                currentState = new DomainState();
            }

            // Numeric accumulation (e.g., 300.0 + 50.0 = 350.0)
            currentState.sum += event.amount;

            // Schema evolution
            if (event.dynamicDimension != null && !event.dynamicDimension.isEmpty()) {
                currentState.attributes.put(event.dynamicDimension, "active_blue_green_kafka");
            }
            currentState.attributes.put("migration_strategy", "dual_pipeline_kafka_blue_green");

            state.update(currentState);

            OutputResult res = new OutputResult();
            res.key = event.key;
            res.totalSum = currentState.sum;
            res.pipelineVersion = "GREEN_V2_KAFKA";
            res.attributes = currentState.attributes;

            out.collect(res);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Configure Kafka Source with independent consumer group for Green pipeline
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user-events-topic")
                .setGroupId("blue-green-pipeline-green-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Blue-Green Source")
                .flatMap(new BlueGreenKafkaFlatMap())
                .print(); // In production, replace with idempotent JDBC/Sink

        env.execute("Kafka Blue-Green State Migration Job");
    }
}