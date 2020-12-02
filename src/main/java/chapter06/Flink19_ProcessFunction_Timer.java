package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink19_ProcessFunction_Timer {
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
//                            // TODO 1.注册定时器
/*                                long time = System.currentTimeMillis() + 5000;
                                ctx.timerService().registerProcessingTimeTimer(time);
                                out.collect("注册了定时器，ts=" + new Timestamp(time));*/
                                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);

                                //TODO 定时器 注册 and 触发 源码分析（事件时间的定时器）
//                                	=> 1、注册 eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
//		                                    => 队列添加的时候，会进行去重，如果是同一组，重复注册了相同时间的定时器，只会添加一个到队列里
//                                          => 每个分组是隔离的， 即使注册的时间是相同的，但是每个分组照样有一个
//
//                                 =>  2、触发 timer.getTimestamp() <= time ==========> 定时的时间 <= watermark

                            }

                            /**
                             * // TODO 2.定义 onTimer 方法 - 定时器触发 之后 的处理逻辑
                             * @param timestamp 定时器触发的时间，也就是 定的那个时间
                             * @param ctx   上下文
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                                System.out.println("定时器触发了, onTimer ts = " + new Timestamp(timestamp));
                                System.out.println("定时器触发了, onTimer ts = " + timestamp);
                            }
                        }
                )
                .print();


        env.execute();
    }
}
