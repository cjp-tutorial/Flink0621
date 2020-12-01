package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink14_Watermark_Sumary {
    public static void main(String[] args) throws Exception {
        // TODO 1.对 watermark 的理解（概念、原理）
        // 1.解决 乱序的 问题
        // 2.表示 事件时间的 进展
        // 3.是一个 特殊的事件时间 的数据（类里面，就一个 时间戳 属性）
        // 4.单调递增（不减）
        // 5.用来 触发 窗口的 计算、关窗
        // 6.认为 在它时间之前的 数据都 处理过了

        // TODO 2.watermark的生成逻辑
        // 1. 升序的： watermark = maxTs - 1ms
        // 2. 乱序的： watermark = maxTs - 乱序等待时间 - 1ms

        // TODO 3.watermark的生成方式
        // 1.periodic（周期性）：默认是这种方式，默认周期为 200ms
        //      => 升序：WatermarkStrategy.<T>forMonotonousTimestamps()
        //      => 乱序：WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration乱序程度)
        // 2.punctuated(打点式、间歇性)： 来一条生成一次

        // TODO 4.watermark多并行度下的确定
        // 以 最小的 为准

        // TODO 5.watermark的传递
        // 一般在source指定watermark的生成
        // watermark作为一个特殊的时间数据，插入流里，随着流而向下游传递
        // 理解：
        //      多 对 一：选最小
        //      一 对 多：广播
        //      多 对 多：结合 上面 两种
    }
}
