package com.statemigration.flink.versionedstate;

import java.util.HashMap;
import java.util.Map;

public class VersionedStateModel {

    // V1 Legacy State Structure
    public static class V1AggregateState {
        public double total;
        public V1AggregateState() {}
    }

    // V2 Evolved State Structure
    public static class V2AggregateState {
        public double sum = 0.0;
        public String newConfigurableField = "default";
        public Map<String, Object> attributes = new HashMap<>();
        public V2AggregateState() {}
    }

    // State Wrapper containing the schema version flag
    public static class StateWrapper<T> {
        public int schemaVersion;
        public T payload;

        public StateWrapper() {}

        public StateWrapper(int schemaVersion, T payload) {
            this.schemaVersion = schemaVersion;
            this.payload = payload;
        }
    }
}