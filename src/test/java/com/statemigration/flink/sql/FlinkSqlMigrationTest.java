package com.statemigration.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import static org.apache.flink.table.api.Expressions.$;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlinkSqlMigrationTest {

    // Simulating a legacy V1 event structure (only user_key and amount)
    public static class V1Event {
        public String user_key;
        public double amount;

        public V1Event() {}
        public V1Event(String user_key, double amount) {
            this.user_key = user_key;
            this.amount = amount;
        }
    }

    @Test
    public void testFlinkSqlEvolutionFromV1ToV2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. Ingest historical V1 data stream into Flink Table Environment
        List<V1Event> legacyV1Data = Arrays.asList(
                new V1Event("user_1", 100.0),
                new V1Event("user_1", 50.0),
                new V1Event("user_2", 200.0)
        );

        DataStream<V1Event> v1Stream = env.fromCollection(legacyV1Data);

        // Register the V1 data stream as a virtual SQL table
        tEnv.createTemporaryView("legacy_events_v1", v1Stream,
                $("user_key"),
                $("amount")
        );

        // 2. Query the data using the evolved V2 projection logic
        // (Adding the new configurable field dynamically during query time, simulating schema evolution)
        Table evolvedTable = tEnv.sqlQuery(
                "SELECT " +
                        "  user_key, " +
                        "  SUM(amount) AS total_sum, " +
                        "  'migrated_sql_v2' AS new_configurable_field " +
                        "FROM legacy_events_v1 " +
                        "GROUP BY user_key"
        );

        // 3. Collect results into a local list to assert correctness
        List<Row> results = FlinkSqlTestUtils.collectTableRows(tEnv, evolvedTable);

        // 4. Assert that sums are correct and the evolved V2 field is successfully populated
        assertEquals(2, results.size());

        for (Row row : results) {
            String userKey = row.getFieldAs("user_key");
            Double totalSum = row.getFieldAs("total_sum");
            String newField = row.getFieldAs("new_configurable_field");

            assertEquals("migrated_sql_v2", newField);
            if ("user_1".equals(userKey)) {
                assertEquals(150.0, totalSum, 0.001);
            } else if ("user_2".equals(userKey)) {
                assertEquals(200.0, totalSum, 0.001);
            }
        }
    }
}