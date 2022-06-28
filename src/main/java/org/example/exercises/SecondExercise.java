package org.example.exercises;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.LinearRoadSource;
import org.example.SpeedEvent;
import org.example.WatermarkAssigner;
import org.example.avgspeed.AverageSpeed;
import org.example.avgspeed.AverageSpeedCalculator;
import org.example.minmax.Diff;
import org.example.minmax.DiffCalculator;

//Calculate the average speed per vehicle in the last 5 minutes
public class SecondExercise {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        LinearRoadSource linearRoadSource = new LinearRoadSource("/home/ekeshi/Documents/diploma/first-flink-job/src/main/java/org/example/linear2.dat");



        DataStream<SpeedEvent> speedEventDataStream = env.addSource(linearRoadSource).assignTimestampsAndWatermarks(new WatermarkAssigner());

        DataStream<AverageSpeed> averageSpeedDataStream = speedEventDataStream.
                keyBy(speed-> speed.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(5),Time.seconds(10)))
                .apply(new AverageSpeedCalculator());
        averageSpeedDataStream.print();

        env.execute("Second exercise find avg speed every 5 min");
    }
}
