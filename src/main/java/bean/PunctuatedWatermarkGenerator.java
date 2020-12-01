package bean;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 15:23
 */
public class PunctuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {


    /**
     * 来一条数据，执行一次这个方法
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("on Event");
        output.emitWatermark(new Watermark(eventTimestamp));
    }

    /**
     * 每隔一个周期，执行这个方法
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
