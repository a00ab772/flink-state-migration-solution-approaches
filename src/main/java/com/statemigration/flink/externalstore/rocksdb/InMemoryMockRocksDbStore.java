package com.statemigration.flink.externalstore.rocksdb;

import com.statemigration.flink.externalstore.ExternalizedStateJob;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryMockRocksDbStore implements ExternalizedStateJob.ExternalKeyValueStore {
    // In-memory key-value store simulating RocksDB storage
    private final ConcurrentMap<String, byte[]> memoryStore = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws Exception {
        return memoryStore.get(key);
    }

    @Override
    public void put(String key, byte[] value) throws Exception {
        memoryStore.put(key, value);
    }

    // Helper method for test assertions on raw bytes
    public ConcurrentMap<String, byte[]> getMemoryStore() {
        return memoryStore;
    }
}