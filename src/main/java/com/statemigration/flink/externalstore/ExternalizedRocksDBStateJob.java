package com.statemigration.flink.externalstore;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ExternalizedRocksDBStateJob {

    public static class StreamEvent {
        private String userId;
        private String action;
        public StreamEvent() {}
        public StreamEvent(String userId, String action) { this.userId = userId; this.action = action; }
        public String getUserId() { return userId; }
        public String getAction() { return action; }
    }

    public static class ExternalizedRocksDBStateFunction extends RichAsyncFunction<StreamEvent, String> {
        private transient RocksDB db;
        private transient Options options;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Load native RocksDB libraries
            RocksDB.loadLibrary();
            options = new Options().setCreateIfMissing(true);

            // Open local embedded RocksDB instance on worker node disk
            String dbPath = "/tmp/flink-embedded-rocksdb-" + getRuntimeContext().getIndexOfThisSubtask();
            db = RocksDB.open(options, dbPath);
        }

        @Override
        public void asyncInvoke(StreamEvent event, ResultFuture<String> resultFuture) {
            CompletableFuture.supplyAsync(() -> {
                try {
                    byte[] keyBytes = event.getUserId().getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = db.get(keyBytes);

                    String stateVal = valueBytes == null ? "{}" : new String(valueBytes, StandardCharsets.UTF_8);

                    // Simulate state update & write back
                    String newStateVal = "{\"last_action\":\"" + event.getAction() + "\"}";
                    db.put(keyBytes, newStateVal.getBytes(StandardCharsets.UTF_8));

                    return "User " + event.getUserId() + " | Previous State: " + stateVal;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).thenAccept(result -> resultFuture.complete(Collections.singleton(result)));
        }

        @Override
        public void close() throws Exception {
            if (db != null) db.close();
            if (options != null) options.close();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AsyncDataStream.unorderedWait(
                env.fromElements(
                        new StreamEvent("user_1", "click"),
                        new StreamEvent("user_1", "purchase")
                ),
                new ExternalizedRocksDBStateFunction(),
                1000, TimeUnit.MILLISECONDS,
                100
        ).print();

        env.execute("Flink Externalized RocksDB State Async Job");
    }
}
