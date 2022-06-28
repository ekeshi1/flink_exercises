package org.example.exercises;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.DoubleValue;
import org.example.LinearRoadSource;
import org.example.SpeedEvent;
import org.example.WatermarkAssigner;
import org.example.minmax.Diff;
import org.example.minmax.DiffCalculator;

//Select all Speed Events with a speed greater than 50.

public class FirstExercise {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        LinearRoadSource linearRoadSource = new LinearRoadSource("/home/ekeshi/Documents/diploma/first-flink-job/src/main/java/org/example/linear2.dat");



        DataStream<SpeedEvent> speedEventDataStream = env.addSource(linearRoadSource).assignTimestampsAndWatermarks(new WatermarkAssigner());
        double lowestSpeed = 50d;

		DataStream<SpeedEvent> filteredSpeedEvents =
				speedEventDataStream.filter(speedEvent -> speedEvent.f2.compareTo(new DoubleValue(lowestSpeed))>0);


        filteredSpeedEvents.print();

        env.execute("First exercise");
    }
}
