package org.example.minmax;

import org.example.SpeedEvent;

import java.util.ArrayList;
import java.util.List;
public class Diff {

    Double min;
    Double max;
    Double diff;
    String id;
    Integer totalCount;


    public Diff(Double min, Double max, Double diff, String id, Integer totalCount) {
        this.min = min;
        this.max = max;
        this.diff = diff;
        this.id = id;
        this.totalCount = totalCount;

    }

    @Override
    public String toString() {
        return "MinAndMAx{" +
                "min=" + min +
                ", max=" + max +
                ", diff=" + diff +
                ", id='" + id + '\'' +
                ", totalCount=" + totalCount +
                '}';
    }
}
