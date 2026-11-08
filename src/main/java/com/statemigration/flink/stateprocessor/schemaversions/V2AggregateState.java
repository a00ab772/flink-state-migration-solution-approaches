package com.statemigration.flink.stateprocessor.schemaversions;

import java.io.Serializable;
import java.util.Objects;

public class V2AggregateState implements Serializable {
    private static final long serialVersionUID = 1L;

    public double sum;
    public String newConfigurableField;

    public V2AggregateState() {}

    public V2AggregateState(double sum, String newConfigurableField) {
        this.sum = sum;
        this.newConfigurableField = newConfigurableField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        V2AggregateState that = (V2AggregateState) o;
        return Double.compare(that.sum, sum) == 0 &&
                Objects.equals(newConfigurableField, that.newConfigurableField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, newConfigurableField);
    }
}