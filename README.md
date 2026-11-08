# Flink State Migration Challenge

## Problem Statement

State migration is a critical aspect of stream processing applications, especially when dealing with evolving data schemas or application logic.

Apache Flink provides mechanisms for state management, but migrating state between different versions of an application can be challenging, because state is serialized into binary formats tied to specific serializers and schema definitions.

When your end-users dynamically redefine their queries, aggregations, and filters through configuration, changing the schema of the state (e.g., adding a field to an aggregated window state or changing a key type) breaks backward compatibility during a savepoint restore.

This project explores various approaches to state migration in Flink, aiming to provide insights and best practices.

# Strategies to solve the problem

## 1. **Custom State Migration Logic**: Implementing custom logic to transform state from the old schema to the new schema during the restore process.

If the state schema changes are minor, you can implement custom state migration logic in your Flink application.

This involves writing code that reads the old state format and transforms it into the new format during the restore process.

* How it works: Do not store raw domain objects directly in Flink’s `ValueState` or `ReducingState`. Instead, wrap your state in a generic versioned wrapper (e.g., `StateWrapper<T>` containing a `schemaVersion` integer).

* Migration logic: When restoring from a savepoint, your `RichFunction.open()` method inspects the `schemaVersion`. If the stored version is older than the current config version, you apply an upcaster function to transform the old binary/POJO layout into the new schema structure on the fly before processing new records.

* Limitation: Works well for additive changes or simple type widenings, but becomes brittle if the core grouping key or state structure fundamentally changes.

[UpcastingJob.java](src/main/java/com/statemigration/flink/upcasting/UpcastingJob.java)

[UpcastingJobTest.java](src/test/java/com/statemigration/flink/upcasting/UpcastingJobTest.java)

## 2. **state agnostic abstract dsl architecture**: Designing your application to be agnostic to state schema changes by using an abstract DSL that decouples the state representation from the application logic.

Instead of mapping user configs directly to complex, strongly-typed Flink stateful operators (which forces state schema changes whenever the config changes), abstract the runtime into a fixed, generic state topology.

* How it works: Design a universal state schema for all user-defined use cases. For example, instead of creating custom state fields for every user aggregation, use a flexible key-value or columnar state container within Flink (like a `MapState<String, String>` or a generic JSON/Protobuf byte array).

* Config translation: The user configuration is compiled into execution parameters or evaluation rules (using a lightweight expression engine like MVEL, Janino, or CEL) evaluated *against* the generic state, rather than changing the Flink operator's state schema itself.

* Benefit: The Flink state schema never changes. You only update the configuration or the evaluation logic, allowing seamless savepoint restores because the underlying Flink state structure remains identical (`MapState<String, byte[]>`).

[GenericDslJob.java](src/main/java/com/statemigration/flink/schemaagnostic/GenericDslJob.java)

[GenericDslJobTest.java](src/test/java/com/statemigration/flink/schemaagnostic/GenericDslJobTest.java)

## 3. **Externalized State Management**: Storing state externally in a database or key-value store, allowing for more flexible schema evolution.

Moving complex or frequently migrated state out of Flink's native managed state and into an external, schema-flexible key-value store (such as Redis, DynamoDB, or PostgreSQL) can bypass Flink's rigid state serialization constraints.

* How it works: Use Flink purely as a stateless or lightly-stateful stream processor (using Flink only for windowing triggers or transient coordination), and fetch/persist the heavy domain state from an external database inside your `RichFlatMapFunction` or `AsyncFunction`.

* Benefit: External databases allow arbitrary schema migrations, ALTER TABLE commands, and JSON document evolution without being bound to Flink's binary savepoint constraints.

* Limitation: This approach introduces external dependencies, potential latency, and operational complexity.

[ExternalizedStateJob.java](src/main/java/com/statemigration/flink/externalstore/ExternalizedStateJob.java)

[ExternalizedStateJobTest.java](src/test/java/com/statemigration/flink/externalstore/ExternalizedStateJobTest.java)

## 4. **Dual-pipeline blue-green state migration**: Running two parallel pipelines (old and new) during a migration period, allowing for gradual state transfer and validation.

When a configuration change introduces a breaking state change (e.g., changing the windowing strategy or reshaping the aggregate key entirely), upcasting is impossible. You must migrate the data cleanly.

* How it works:

a. Spin up a new Flink job running the new configuration version, reading from the same source Kafka topics (configured with a sufficiently long retention period or a replayable offset/timestamp).

b. Run the old pipeline and the new pipeline in parallel for a transition window.

c. For historical state bootstrapping, use a batch job or a side-load mechanism to read existing state/sinks, transform it into the new schema format, and inject it into the new pipeline's state backend or sink storage.

d. Once caught up, drain and terminate the old pipeline.

This is considered in literature as the gold standard for zero-downtime, high-risk migrations in enterprise streaming architectures.

Why it shines over the other approaches is that, when breaking changes are too radical for Upcasting (Strategy 1) or when externalizing state (Strategy 3) isn't enough because the topology itself changes (e.g., changing key-by groupings, window slide/sizes, or data structures entirely), modifying savepoints becomes mathematically impossible as you most probably have experienced with the real product platform that you are building. The beauty of this approach is that, running parallel pipelines bypasses Flink's internal savepoint dependency graph completely.

Of course this approach also comes with a Trade-off: It introduces operational complexity. You must manage dual consumer groups on Kafka, handle idempotent sinks so downstream systems don't receive duplicate data during the transition window, and build a side-loading/bootstrap mechanism to backfill historical state into the new topology.

(WIP)

[StateMigrationBlueGreenJob.java](src/main/java/com/statemigration/flink/bluegreen/StateMigrationBlueGreenJob.java)

[StateMigrationBlueGreenJobTest.java](src/main/java/com/statemigration/flink/bluegreen/StateMigrationBlueGreenJobTest.java)

## 5. **Versioned State**: Maintaining multiple versions of state and handling them appropriately during restore.

This approach handles schema evolution seamlessly in Apache Flink by wrapping domain state with a explicit schema version indicator (like the StateWrapper<T> pattern we used earlier).

This ensures that when your pipeline restarts or restores from a savepoint created by an older version of your code, Flink knows precisely how to upcast or deserialize legacy state structures without failing.

Pros:

* Explicit & Safe: By inspecting schemaVersion, the job precisely identifies the data structure before processing, avoiding cryptic StateSerializationException or deserialization failures on restore.
* Native Integration: Everything stays within Flink's native checkpoint and savepoint mechanism—no external databases or infrastructure required.
* Deterministic Upcasting: You control the exact migration path (e.g., mapping old total to new sum) explicitly in code, preventing data loss or silent type corruption.

Cons:

* ConsBoilerplate Overhead: Every state object must be wrapped inside a generic container (StateWrapper<T>), requiring extra code for serialization and casting.
* Maintenance Debt: As your application evolves through multiple versions (V1 -> V2 -> V3 -> V4), the upcasting logic can accumulate nested conditional blocks.
* Requires Restart/Savepoint: To apply the migration, you must stop the pipeline, take a savepoint, update the code with the new version handler, and restore.

[VersionedStateModel.java](src/main/java/com/statemigration/flink/versionedstate/VersionedStateModel.java)

[VersionedStateFlatMap.java](src/main/java/com/statemigration/flink/versionedstate/VersionedStateFlatMap.java)

[VersionedStateTest.java](src/test/java/com/statemigration/flink/versionedstate/VersionedStateTest.java)

(Work in progress ....)

## 6. **State Serialization**: Using a serialization framework that supports schema evolution, such as Avro or Protobuf.

## 7. **State Migration Tooling**: Leveraging Flink's state migration tooling, such as the State Processor API, to facilitate state migration.

...


## Project setup

```shell
user@DESKTOP-SCNMK3I UCRT64 ~/Documents/FlinkProjects
Sat Nov 07 06:57:16
$ mvn archetype:generate \
  -DarchetypeGroupId=org.apache.flink \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.18.1 \
  -DgroupId=com.statemigration.flink \
  -DartifactId=flink-state-migration-solution-approaches \
  -Dversion=1.0-SNAPSHOT \
  -Dpackage=com.statemigration.flink \
  -DinteractiveMode=false
[INFO] Scanning for projects...
Downloading from central: https://repo.maven.apache.org/maven2/org/codehaus/mojo/maven-metadata.xml
Downloading from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-metada
ta.xml
Downloaded from central: https://repo.maven.apache.org/maven2/org/codehaus/mojo/maven-metadata.xml (
20 kB at 9.1 kB/s)
Downloaded from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-metadata.xml (10 kB at 4.6 kB/s)
Downloading from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-archetype-plugin/maven-metadata.xml
Downloaded from central: https://repo.maven.apache.org/maven2/org/apache/maven/plugins/maven-archetype-plugin/maven-metadata.xml (1.1 kB at 12 kB/s)
[INFO]
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------
[INFO]
[INFO] >>> archetype:3.4.1:generate (default-cli) > generate-sources @ standalone-pom >>>
[INFO]
[INFO] <<< archetype:3.4.1:generate (default-cli) < generate-sources @ standalone-pom <<<
[INFO]
[INFO]
[INFO] --- archetype:3.4.1:generate (default-cli) @ standalone-pom ---
[INFO] Generating project in Batch mode
Downloading from central: https://repo.maven.apache.org/maven2/archetype-catalog.xml
Downloaded from central: https://repo.maven.apache.org/maven2/archetype-catalog.xml (18 MB at 53 MB/s)
[INFO] Archetype repository not defined. Using the one from [org.apache.flink:flink-quickstart-java:2.3.0] found in catalog remote
[INFO] ----------------------------------------------------------------------------
[INFO] Using following parameters for creating project from Archetype: flink-quickstart-java:1.18.1
[INFO] ----------------------------------------------------------------------------
[INFO] Parameter: groupId, Value: com.statemigration.flink
[INFO] Parameter: artifactId, Value: flink-state-migration-solution-approaches
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[INFO] Parameter: package, Value: com.statemigration.flink
[INFO] Parameter: packageInPathFormat, Value: com/statemigration/flink
[INFO] Parameter: package, Value: com.statemigration.flink
[INFO] Parameter: groupId, Value: com.statemigration.flink
[INFO] Parameter: artifactId, Value: flink-state-migration-solution-approaches
[INFO] Parameter: version, Value: 1.0-SNAPSHOT
[WARNING] CP Don't override file C:\Users\user\Documents\FlinkProjects\flink-state-migration-solution-approaches\src\main\resources
[INFO] Project created from Archetype in dir: C:\Users\user\Documents\FlinkProjects\flink-state-migration-solution-approaches
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  5.025 s
[INFO] Finished at: 2026-11-07T06:58:55+01:00
[INFO] ------------------------------------------------------------------------
```

