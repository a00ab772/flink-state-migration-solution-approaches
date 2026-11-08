package com.statemigration.flink.typeserializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class V2AggregateStateSerializer extends TypeSerializer<V2AggregateState> {

    private final int serializerVersion;

    public V2AggregateStateSerializer() {
        this.serializerVersion = 2;
    }

    public V2AggregateStateSerializer(int serializerVersion) {
        this.serializerVersion = serializerVersion;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<V2AggregateState> duplicate() {
        return this;
    }

    @Override
    public V2AggregateState createInstance() {
        return new V2AggregateState(0.0, "default_init");
    }

    @Override
    public V2AggregateState copy(V2AggregateState from) {
        return new V2AggregateState(from.sum, from.newConfigurableField);
    }

    @Override
    public V2AggregateState copy(V2AggregateState from, V2AggregateState reuse) {
        reuse.sum = from.sum;
        reuse.newConfigurableField = from.newConfigurableField;
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(V2AggregateState record, DataOutputView target) throws IOException {
        target.writeDouble(record.sum);
        target.writeUTF(record.newConfigurableField);
    }

    @Override
    public V2AggregateState deserialize(DataInputView source) throws IOException {
        double sum = source.readDouble();
        String field;

        if (serializerVersion == 1) {
            // Legacy V1 stream: did not write newConfigurableField, assign default migration value
            field = "migrated_v1_default";
        } else {
            field = source.readUTF();
        }
        return new V2AggregateState(sum, field);
    }

    @Override
    public V2AggregateState deserialize(V2AggregateState reuse, DataInputView source) throws IOException {
        reuse.sum = source.readDouble();
        reuse.newConfigurableField = (serializerVersion == 1) ? "migrated_v1_default" : source.readUTF();
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        double sum = source.readDouble();
        target.writeDouble(sum);
        if (serializerVersion >= 2) {
            target.writeUTF(source.readUTF());
        } else {
            target.writeUTF("migrated_v1_default");
        }
    }

    @Override
    public TypeSerializerSnapshot<V2AggregateState> snapshotConfiguration() {
        return new V2AggregateStateSerializerSnapshot();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof V2AggregateStateSerializer && ((V2AggregateStateSerializer) obj).serializerVersion == this.serializerVersion;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(serializerVersion);
    }
}