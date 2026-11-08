package com.statemigration.flink.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkSqlTestUtils {
    public static List<Row> collectTableRows(StreamTableEnvironment tEnv, Table table) throws Exception {
        Map<String, Row> latestRowsByGroup = new HashMap<>();

        try (CloseableIterator<Row> iterator = tEnv.toChangelogStream(table).executeAndCollect()) {
            while (iterator.hasNext()) {
                Row row = iterator.next();
                org.apache.flink.types.RowKind kind = row.getKind();

                // Process inserts and updates in the changelog stream to maintain the latest state per user_key
                if (kind == org.apache.flink.types.RowKind.INSERT || kind == org.apache.flink.types.RowKind.UPDATE_AFTER) {
                    String userKey = row.getFieldAs("user_key");
                    latestRowsByGroup.put(userKey, row);
                }
            }
        }

        // Return only the final consolidated state per user
        return new ArrayList<>(latestRowsByGroup.values());
    }
}