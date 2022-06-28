package org.example.avgspeed;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.example.SpeedEvent;

public class AverageSpeedCalculator implements WindowFunction<SpeedEvent, AverageSpeed, StringValue, TimeWindow> {
    @Override
    public void apply(StringValue carId, TimeWindow timeWindow, Iterable<SpeedEvent> iterable, Collector<AverageSpeed> outputCollector) throws Exception {
            int count = 0;
            double sum = 0.0;
            for (SpeedEvent r : iterable) {
                count++;
                sum += r.f2.getValue();
            }
            double avgSpeed = sum / count;

            // emit a SensorReading with the average temperature
            outputCollector.collect(new AverageSpeed(carId.getValue(), timeWindow.getEnd(), avgSpeed, count));
        }
    }

