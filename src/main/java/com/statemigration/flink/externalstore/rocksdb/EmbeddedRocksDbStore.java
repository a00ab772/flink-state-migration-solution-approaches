package com.statemigration.flink.externalstore.rocksdb;

import com.statemigration.flink.externalstore.ExternalizedStateJob;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

/*
 * EmbeddedRocksDbStore is a simple wrapper around RocksDB to provide an external key-value store for Flink state.
 * It allows storing and retrieving byte arrays by string keys, simulating an external database.
 * This implementation uses RocksDB as the underlying storage engine.
 *
 * It depends on the RocksDB JNI library, which must be included in the project dependencies.
 * I opt out not to install it, as this is my job laptop, but feel free to install it if you want to run the RocksDB-based tests.
 *
 */
public class EmbeddedRocksDbStore implements ExternalizedStateJob.ExternalKeyValueStore, AutoCloseable {
    private final RocksDB db;
    private final Options options;
    private final File dbDir;

    public EmbeddedRocksDbStore(String tempDirPath) throws Exception {
        // Ensure RocksDB native library is loaded
        RocksDB.loadLibrary();

        dbDir = new File(tempDirPath, "rocksdb-data-" + System.currentTimeMillis());
        options = new Options().setCreateIfMissing(true);

        // Open the real embedded RocksDB instance on local disk
        db = RocksDB.open(options, dbDir.getAbsolutePath());
    }

    @Override
    public byte[] get(String key) throws Exception {
        return db.get(key.getBytes());
    }

    @Override
    public void put(String key, byte[] value) throws Exception {
        db.put(key.getBytes(), value);
    }

    @Override
    public void close() {
        try {
            if (db != null) db.close();
            if (options != null) options.close();
        } catch (Exception ignored) {}
    }
}