package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink06_TimeCharacteristic_ProcessingTime {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.设置执行环境，指定为 事件时间 语义
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> socketDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });
                // TODO 2.指定 如何从 数据里 提取 事件时间（注意：单位为 ms）
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy
//                                .<WaterSensor>forMonotonousTimestamps()
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<WaterSensor>() {
//                                            @Override
//                                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
//                                                return element.getTs() * 1000L;
//                                            }
//                                        }
//                                )
//                );


        socketDS
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .sum("vc")
                .print();




        env.execute();
    }
}
