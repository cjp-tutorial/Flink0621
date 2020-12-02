package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink17_Watermark_SideOutput {
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

        // TODO 迟到数据处理 - 侧输出流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("wuyanzu") {
        };

        SingleOutputStreamOperator<String> resultDS = socketDS
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
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
                );


        // TODO 从 主流里 获取 侧输出流
        DataStream<WaterSensor> sideOutput = resultDS.getSideOutput(outputTag);

        resultDS.print("result");
        sideOutput.print("side");

        // 怎么 把迟到的数据 跟 原来的结果进行合并
        // union \ connect
        // 1.判断 迟到的数据 属于哪个窗口，
        // 2.取出 主流里 对应窗口的 结果
        // 3.计算 并更新结果


        env.execute();
    }
}
