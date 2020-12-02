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
public class Flink16_Watermark_AllowLateness {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> socketDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((sensor, recordTs) -> sensor.getTs() * 1000L)
                );

        // TODO 窗口允许迟到 - 开窗之后调用 allowedLateness（Time）
        // 当 wm >= 窗口最大时间戳 时，触发窗口的计算，但是不会关窗
        // 当 窗口最大时间戳 < wm < 窗口最大时间戳 + 允许迟到时间， 每来一条 迟到的数据（属于本窗口的数据）都会触发一次计算
        // 当 wm >= 窗口最大时间戳 + 允许迟到时间, 会关闭窗口， 再有迟到的数据，也不会被计算
        socketDS
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
//                .allowedLateness(Time.seconds(2))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                out.collect("======================================="
                                        + "\nkey=" + s
                                        + "\n当前窗口是[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + "\n一共有" + elements.spliterator().estimateSize() + "条数据"
                                        + "\nwatermark=" + context.currentWatermark()
                                        + "\n======================================\n\n");
                            }
                        }
                )
                .print();

        env.execute();
    }
}
