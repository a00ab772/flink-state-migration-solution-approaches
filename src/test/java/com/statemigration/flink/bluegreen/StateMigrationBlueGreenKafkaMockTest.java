package com.statemigration.flink.bluegreen;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StateMigrationBlueGreenKafkaMockTest {

    // A thread-safe sink collection to gather test outputs from the Flink pipeline
    public static class CollectingSink implements SinkFunction<StateMigrationBlueGreenKafkaJob.OutputResult> {
        public static final List<StateMigrationBlueGreenKafkaJob.OutputResult> VALUES =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StateMigrationBlueGreenKafkaJob.OutputResult value, Context context) {
            VALUES.add(value);
        }
    }

    @Test
    public void testBlueGreenPipelineWithMockedKafkaMessages() throws Exception {
        // Clear previous runs
        CollectingSink.VALUES.clear();

        // Arrange: Set up Flink local execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Single thread for deterministic test execution

        // Mock Kafka input messages as raw JSON strings (simulating messages arriving from Kafka topics)
        List<String> mockKafkaMessages = List.of(
                "{\"key\":\"user_kafka_1\", \"amount\":100.0, \"dynamicDimension\":\"tier\"}",
                "{\"key\":\"user_kafka_1\", \"amount\":50.0, \"dynamicDimension\":\"tier\"}"
        );

        // 1. Use Flink's collection source to feed mock Kafka messages instead of a real Kafka broker
        DataStream<String> mockKafkaSourceStream = env.fromCollection(
                mockKafkaMessages,
                TypeInformation.of(String.class)
        );

        // 2. Attach our stateful Blue-Green FlatMap transformation and collect output via sink.
        //    Extract the key from the json and group with the keyBy() method before the stateful flat map.
        mockKafkaSourceStream
                .keyBy(jsonStr -> {
                    try {
                        JsonNode node = new ObjectMapper().readTree(jsonStr);
                        return node.get("key").asText();
                    } catch (Exception e) {
                        return "unknown_key";
                    }
                })
                .flatMap(new StateMigrationBlueGreenKafkaJob.BlueGreenKafkaFlatMap())
                .addSink(new CollectingSink());

        // Act: Execute the Flink pipeline job locally
        env.execute("Mocked Kafka Blue-Green Test Execution");

        // Assert: Verify state accumulation (100.0 + 50.0 = 150.0) and schema evolution
        assertEquals(2, CollectingSink.VALUES.size());

        // First event output check
        assertEquals("user_kafka_1", CollectingSink.VALUES.get(0).key);
        assertEquals(100.0, CollectingSink.VALUES.get(0).totalSum, 0.001);
        assertEquals("GREEN_V2_KAFKA", CollectingSink.VALUES.get(0).pipelineVersion);
        assertEquals("active_blue_green_kafka", CollectingSink.VALUES.get(0).attributes.get("tier"));

        // Second event output check (accumulated state verification)
        assertEquals("user_kafka_1", CollectingSink.VALUES.get(1).key);
        assertEquals(150.0, CollectingSink.VALUES.get(1).totalSum, 0.001);
        assertEquals("dual_pipeline_kafka_blue_green", CollectingSink.VALUES.get(1).attributes.get("migration_strategy"));
    }
}