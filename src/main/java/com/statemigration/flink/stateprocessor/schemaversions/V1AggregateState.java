package com.statemigration.flink.stateprocessor.schemaversions;

import java.io.Serializable;

public class V1AggregateState implements Serializable {
    private static final long serialVersionUID = 1L;

    public double total;

    public V1AggregateState() {}

    public V1AggregateState(double total) {
        this.total = total;
    }
}