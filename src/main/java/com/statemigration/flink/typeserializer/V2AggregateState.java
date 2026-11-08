package com.statemigration.flink.typeserializer;

import java.io.Serializable;

public class V2AggregateState implements Serializable {
    private static final long serialVersionUID = 1L;

    public double sum;
    public String newConfigurableField;

    public V2AggregateState() {}

    public V2AggregateState(double sum, String newConfigurableField) {
        this.sum = sum;
        this.newConfigurableField = newConfigurableField;
    }
}