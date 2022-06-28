package org.example.avgspeed;

public class AverageSpeed {

    Double value;
    String id;
    Long timestampEnd;

    Integer totalCount;

    public AverageSpeed(String id,  Long timestampEnd,Double value, Integer count) {
        this.value = value;
        this.id = id;
        this.timestampEnd = timestampEnd;
        this.totalCount = count;
    }


    @Override
    public String toString() {
        return "AverageSpeed{" +
                "value=" + value +
                ", id='" + id + '\'' +
                ", timestampEnd=" + timestampEnd +
                ", totalCount=" + totalCount +
                '}';
    }
}
