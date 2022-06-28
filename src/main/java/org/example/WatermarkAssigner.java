package org.example;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WatermarkAssigner implements AssignerWithPeriodicWatermarks<SpeedEvent> {
    long maxTs;


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs + 1000   );
    }



    @Override
    public long extractTimestamp(SpeedEvent speedEvent, long l) {
        maxTs=Math.max(maxTs, speedEvent.getTimestamp());
        return speedEvent.getTimestamp();
    }
}
