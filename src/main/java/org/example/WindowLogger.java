package org.example;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


//just to log and check if windows are moving correctly.
public class WindowLogger implements WindowFunction<SpeedEvent, List<SpeedEvent>, StringValue, TimeWindow> {

    @Override
    public void apply(StringValue stringValue, TimeWindow timeWindow, Iterable<SpeedEvent> iterable, Collector<List<SpeedEvent>> collector) throws Exception {
        List<SpeedEvent> result = new ArrayList<SpeedEvent>();
        for (SpeedEvent speedEvent : iterable) {
            result.add(speedEvent);
        }

        Collections.sort(result, new Comparator<SpeedEvent>() {
            @Override
            public int compare(SpeedEvent u1, SpeedEvent u2) {
                return u1.f1.compareTo(u2.f1);
            }
        });

        collector.collect(result);
    }
}
