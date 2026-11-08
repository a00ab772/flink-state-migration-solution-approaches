package com.statemigration.flink.externalstore.redis;

import com.statemigration.flink.externalstore.ExternalizedStateJob;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

public class RedisStore implements ExternalizedStateJob.ExternalKeyValueStore, AutoCloseable {
    private final RedisClient redisClient;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> syncCommands;

    public RedisStore(String redisUri) {
        this.redisClient = RedisClient.create(redisUri);
        // Connect using ByteArrayCodec for raw byte array storage
        this.connection = redisClient.connect(new ByteArrayCodec());
        this.syncCommands = this.connection.sync();
    }

    public RedisStore(RedisClient client, StatefulRedisConnection<byte[], byte[]> conn) {
        this.redisClient = client;
        this.connection = conn;
        this.syncCommands = this.connection.sync();
    }

    @Override
    public byte[] get(String key) throws Exception {
        return syncCommands.get(key.getBytes());
    }

    @Override
    public void put(String key, byte[] value) throws Exception {
        syncCommands.set(key.getBytes(), value);
    }

    @Override
    public void close() {
        if (connection != null) connection.close();
        if (redisClient != null) redisClient.shutdown();
    }
}