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
public class Flink15_Watermark_FileIssue {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 读文件的问题
        // flink会在 文件读取结束之后， 把 watermark 设置为 Long的最大值
        //  => 为了保证所有的 窗口都被触发， 所有的数据都被计算

        // TODO 为什么所有窗口一起触发？
        //  => 因为 watermark 周期是200ms生成一次，默认值是 Long的最小值
        //  => 读取完文件，还不够200ms，没有窗口触发， 结束的时候，watermark被设置为 Long的最大值，一下子所有窗口都触发
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

        socketDS
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                out.collect("=======================================\nkey=" + s
                                        + "\n当前窗口是[" + context.window().getStart() + "," + context.window().getEnd() + ")"
                                        + "\n一共有" + elements.spliterator().estimateSize() + "条数据,"
                                        + "\nwatermark=" + context.currentWatermark()
                                        + "\n======================================\n\n");
                            }
                        }
                )
                .print();


        env.execute();
    }
}
