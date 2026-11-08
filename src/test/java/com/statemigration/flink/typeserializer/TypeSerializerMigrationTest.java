package com.statemigration.flink.typeserializer;

import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TypeSerializerMigrationTest {

    @Test
    public void testCustomTypeSerializerSerializationAndDeserialization() throws Exception {
        V2AggregateStateSerializer serializer = new V2AggregateStateSerializer();
        V2AggregateState originalState = new V2AggregateState(450.50, "custom_serializer_test");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(originalState, out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
        V2AggregateState deserializedState = serializer.deserialize(in);

        assertNotNull(deserializedState);
        assertEquals(450.50, deserializedState.sum, 0.001);
        assertEquals("custom_serializer_test", deserializedState.newConfigurableField);
    }

    @Test
    public void testTypeSerializerSnapshotCompatibility() throws Exception {
        V2AggregateStateSerializer serializer = new V2AggregateStateSerializer();
        V2AggregateStateSerializerSnapshot snapshot = new V2AggregateStateSerializerSnapshot();

        // Test snapshot metadata write and read
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
        snapshot.writeSnapshot(out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

        V2AggregateStateSerializerSnapshot restoredSnapshot = new V2AggregateStateSerializerSnapshot();
        restoredSnapshot.readSnapshot(snapshot.getCurrentVersion(), in, Thread.currentThread().getContextClassLoader());

        // Assert schema compatibility check
        TypeSerializerSchemaCompatibility<V2AggregateState> compatibility =
                restoredSnapshot.resolveSchemaCompatibility(serializer);

        assertTrue(compatibility.isCompatibleAsIs());
    }

    @Test
    public void testV1ToV2StateMigrationDeserialization() throws Exception {
        // Simulate a legacy V1 binary payload (writing ONLY the double sum, e.g., 100.0)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
        out.writeDouble(100.0); // V1 format had no String field

        // Instantiate a V2 serializer configured with restored version = 1
        V2AggregateStateSerializer v2SerializerWithV1Context = new V2AggregateStateSerializer(1);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

        V2AggregateState migratedState = v2SerializerWithV1Context.deserialize(in);

        assertNotNull(migratedState);
        assertEquals(100.0, migratedState.sum, 0.001);
        assertEquals("migrated_v1_default", migratedState.newConfigurableField);
    }
}