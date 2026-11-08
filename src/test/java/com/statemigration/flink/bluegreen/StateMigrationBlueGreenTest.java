package com.statemigration.flink.bluegreen;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class StateMigrationBlueGreenTest {

    // Record para los datos antiguos de prueba
    public static class TestLegacyRecord {
        public String userId;
        public int legacyField;

        public TestLegacyRecord() {}
        public TestLegacyRecord(String userId, int legacyField) {
            this.userId = userId;
            this.legacyField = legacyField;
        }
    }

    // Bootstrap function local para inicializar el savepoint antiguo en el test
    public static class TestLegacyStateBootstrap extends KeyedStateBootstrapFunction<String, TestLegacyRecord> {
        private transient ValueState<Integer> legacyState;

        @Override
        public void open(Configuration parameters) {
            legacyState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("versioned-state", Integer.class)
            );
        }

        @Override
        public void processElement(TestLegacyRecord value, Context ctx) throws Exception {
            legacyState.update(value.legacyField);
        }
    }

    public static class ValidationRecord {
        public String key;
        public int value;

        public ValidationRecord() {}
        public ValidationRecord(String key, int value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class OutputValidationReader extends KeyedStateReaderFunction<String, ValidationRecord> {
        private transient ValueState<Integer> newState;

        @Override
        public void open(Configuration parameters) {
            newState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("new-state", Integer.class)
            );
        }

        @Override
        public void readKey(String key, Context ctx, Collector<ValidationRecord> out) throws Exception {
            Integer val = newState.value();
            if (val != null) {
                out.collect(new ValidationRecord(key, val));
            }
        }
    }

    @Test
    public void testBlueGreenStateMigration(@TempDir Path tempDir) throws Exception {
        String oldSavepointPath = tempDir.resolve("old-savepoint").toUri().toString();
        String newSavepointPath = tempDir.resolve("new-savepoint").toUri().toString();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Crear y escribir el savepoint antiguo de prueba
        DataStream<TestLegacyRecord> dummyInput = env.fromElements(
                new TestLegacyRecord("user-A", 10),
                new TestLegacyRecord("user-B", 20)
        );

        StateBootstrapTransformation<TestLegacyRecord> oldTransformation = OperatorTransformation
                .bootstrapWith(dummyInput)
                .keyBy(record -> record.userId)
                .transform(new TestLegacyStateBootstrap());

        SavepointWriter.newSavepoint(env, 128)
                .withOperator(OperatorIdentifier.forUid("my-operator-uid"), oldTransformation)
                .write(oldSavepointPath);

        env.execute("Generate Old Savepoint Test Phase");

        // 2. Ejecutar la lógica de migración Blue-Green
        StreamExecutionEnvironment migrationEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        SavepointReader savepointReader = SavepointReader.read(
                migrationEnv,
                oldSavepointPath,
                new HashMapStateBackend()
        );

        DataStream<StateMigrationBlueGreenJob.MigratedRecord> migratedStream = savepointReader.readKeyedState(
                OperatorIdentifier.forUid("my-operator-uid"),
                new StateMigrationBlueGreenJob.LegacyStateReader(),
                Types.STRING,
                Types.POJO(StateMigrationBlueGreenJob.MigratedRecord.class)
        );

        StateBootstrapTransformation<StateMigrationBlueGreenJob.MigratedRecord> newTransformation = OperatorTransformation
                .bootstrapWith(migratedStream)
                .keyBy(record -> record.key)
                .transform(new StateMigrationBlueGreenJob.NewStateBootstrap());

        SavepointWriter.newSavepoint(migrationEnv, 128)
                .withOperator(OperatorIdentifier.forUid("new-operator-uid"), newTransformation)
                .write(newSavepointPath);

        migrationEnv.execute("Execute Migration Test Phase");

        // 3. Validar el nuevo savepoint generado
        StreamExecutionEnvironment validationEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        SavepointReader finalReader = SavepointReader.read(validationEnv, newSavepointPath, new HashMapStateBackend());

        DataStream<ValidationRecord> results = finalReader.readKeyedState(
                OperatorIdentifier.forUid("new-operator-uid"),
                new OutputValidationReader(),
                Types.STRING,
                Types.POJO(ValidationRecord.class)
        );

        assertNotNull(results);
    }
}