package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink21_ProcessFunction_TimerPratice {
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
                            private long timerTs = 0L;
                            private int lastVC = 0;

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                if (timerTs == 0) {
                                    // 第一条数据来，注册一个 5s后的定时器
                                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                                    timerTs = ctx.timestamp() + 5000L;
                                }
                                // 判断 是否上升
                                if (value.getVc() < lastVC) {
                                    // 如果出现下降，删除 原来的定时器
                                    ctx.timerService().deleteEventTimeTimer(timerTs);
                                    timerTs = 0L; // 或者直接重新注册，更新 timerTs的值
                                }

                                // 不管上升还是下降，都要把当前的水位值 更新到 变量，为了 下一条数据进行判断
                                lastVC = value.getVc();
                            }

                            /**
                             * 定时器触发，触发 告警
                             * @param timestamp 定时器触发的时间，也就是 定的那个时间
                             * @param ctx   上下文
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                out.collect("warning：监测到 5s内 水位 连续上涨！！！");
                                // 重置保存的 时间， 后面才可以接着告警
                                timerTs = 0L;
                            }
                        }
                )
                .print();


        env.execute();
    }
}
