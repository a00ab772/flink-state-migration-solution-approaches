package com.statemigration.flink.serialization.avro;

import java.io.Serializable;

public class AvroAggregateState implements Serializable {
    private static final long serialVersionUID = 1L;

    public double total = 0.0;
    public String newConfigurableField;

    public AvroAggregateState() {}

    public AvroAggregateState(double total, String newConfigurableField) {
        this.total = total;
        this.newConfigurableField = newConfigurableField;
    }
}