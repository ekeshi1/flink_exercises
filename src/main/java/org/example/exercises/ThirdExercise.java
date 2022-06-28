package org.example.exercises;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.LinearRoadSource;
import org.example.SpeedEvent;
import org.example.WatermarkAssigner;
import org.example.minmax.Diff;
import org.example.minmax.DiffCalculator;
//Calculate the maximum (and minimum) speed of a vehicle in the last 5 minutes
public class ThirdExercise {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        LinearRoadSource linearRoadSource = new LinearRoadSource("/home/ekeshi/Documents/diploma/first-flink-job/src/main/java/org/example/linear2.dat");



        DataStream<SpeedEvent> speedEventDataStream = env.addSource(linearRoadSource).assignTimestampsAndWatermarks(new WatermarkAssigner());
        //finds max, for min use minBy.
        DataStream<Tuple2<String, Double>> minSpeedPerWindowStream = speedEventDataStream.
                keyBy(speed-> speed.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(5),Time.seconds(10)))
                .maxBy("f2")
                        .map(new MapFunction<SpeedEvent, Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> map(SpeedEvent speedEvent) throws Exception {
                               return new Tuple2<String, Double>(speedEvent.f0.getValue(), speedEvent.f2.getValue());
                            }
                        });

        minSpeedPerWindowStream.print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
