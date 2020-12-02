package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink18_ProcessFunction_Keyed {
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

        socketDS
                .keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                                ctx.output(, );   // 侧输出流
                                // ctx.timestamp() 获取的是 数据的 事件时间
                                // ctx.getCurrentKey() 获取当前的key

//                                ctx.timerService().registerEventTimeTimer();
//                                ctx.timerService().deleteEventTimeTimer();
//                                ctx.timerService().registerProcessingTimeTimer();
//                                ctx.timerService().deleteProcessingTimeTimer();

//                                ctx.timerService().currentProcessingTime();
//                                ctx.timerService().currentWatermark();


                                out.collect("当前值="+value
                                        +"\nkey="+ctx.getCurrentKey()
                                        +"\nts="+ctx.timestamp()
                                        +"\npt="+new Timestamp(ctx.timerService().currentProcessingTime())
                                        +"\net="+ctx.timerService().currentWatermark()
                                );

                            }
                        }
                )
                .print();




        env.execute();
    }
}
