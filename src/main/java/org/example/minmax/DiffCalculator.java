package org.example.minmax;

import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.example.SpeedEvent;

public class DiffCalculator extends RichWindowFunction<SpeedEvent, Diff, StringValue, TimeWindow> {



    @Override
    public void apply(StringValue stringValue, TimeWindow timeWindow, Iterable<SpeedEvent> iterable, Collector<Diff> outCollector) throws Exception {


        int count=0;
        Double localMin=Double.MAX_VALUE;
        Double localMAx = Double.MIN_VALUE;

        for(SpeedEvent speedEvent : iterable){
            count++;
            Double val = speedEvent.f2.getValue();


            if(val<localMin) localMin=val;
            if(val>localMAx) localMAx=val;

            //System.out.println("MIN:" +minValue.value());
            //System.out.println("MAX:" + maxValue.value());
            //System.out.println("#################");
        }


        outCollector.collect(new Diff(localMin, localMAx, localMAx-localMin, stringValue.getValue(),count));


    }
}
