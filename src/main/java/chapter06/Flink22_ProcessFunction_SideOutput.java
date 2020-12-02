package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink22_ProcessFunction_SideOutput {
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

        // TODO
        OutputTag<String> alarmTag = new OutputTag<String>("alarm") {
        };

        SingleOutputStreamOperator<WaterSensor> resultDS = socketDS
                .keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                            /**
                             * 当水位高于 5的时候，告警输出到 侧输出流
                             * @param value
                             * @param ctx
                             * @param out
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                //判断 水位值
                                if (value.getVc() > 5) {
                                    // 告警输出到侧输出流
//                                    OutputTag<String> alarmTag = new OutputTag<String>("alarm") {
////                                    };
                                    ctx.output(alarmTag, "监测到水位值超过 5cm！！！！");
                                }
                                out.collect(value);
                            }
                        }
                );


        resultDS.print("result");
//        OutputTag<String> alarmTag = new OutputTag<String>("alarm") {
//        };
        resultDS.getSideOutput(alarmTag).print("alarm");


        env.execute();
    }
}
