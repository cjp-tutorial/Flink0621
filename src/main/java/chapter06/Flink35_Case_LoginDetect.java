package chapter06;

import bean.AdClickLog;
import bean.LoginEvent;
import bean.SimpleAggregateFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/5 10:31
 */
public class Flink35_Case_LoginDetect {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        SingleOutputStreamOperator<LoginEvent> loginDS = env
                .readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((data, ts) -> data.getEventTime() * 1000L)
                );

        // 3.处理数据
        // TODO 缺陷：数据乱序 不能支撑
        // 3.1 按照 统计维度 分组： 用户ID
        KeyedStream<LoginEvent, Long> loginKS = loginDS.keyBy(r -> r.getUserId());
        // 3.2 使用 process 实现
        loginKS
                .process(
                        new KeyedProcessFunction<Long, LoginEvent, String>() {

                            ListState<LoginEvent> failEvents;
                            ValueState<Long> timerTs;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                failEvents = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("failEvents", LoginEvent.class));
                                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                            }

                            @Override
                            public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                                // 判断 是成功 还是 失败
                                if ("success".equals(value.getEventType())) {
                                    // 1.如果来的是成功 => 如果当前有定时器，删除定时器，因为已经不连续
                                    if (timerTs.value() != null) {
                                        ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                        timerTs.clear();
                                        failEvents.clear();
                                    }

                                } else {
                                    // 2.如果来的是失败 => 先判断之前有几次失败了， 再考虑存起来
                                    if (failEvents.get().spliterator().estimateSize() >= 1) {
                                        // 2.1如果之前已经有 1个失败，加上当前1个，已经达到 2个，告警，清空状态
                                        out.collect("用户" + value.getUserId() + "在2s内连续登陆失败达到2次，可能是恶意登陆！！！");
                                        // 已经触发告警规则，删除定时器，清空相关的状态
                                        ctx.timerService().deleteEventTimeTimer(timerTs.value());
                                        failEvents.clear();
                                        timerTs.clear();
                                    } else {
                                        // 2.2 如果之前还没达到1个，那么算上当前的1个，还没达到阈值2个，正常往 List添加
                                        failEvents.add(value);
                                        // 注册一个 2s的定时器
                                        if (timerTs.value() == null) {
                                            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 2000L);
                                            timerTs.update(ctx.timestamp() + 2000L);
                                        }
                                    }
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 判断 list里有几个fail => 不写也行
                                if (failEvents.get().spliterator().estimateSize() >= 2) {
                                    out.collect("用户" + ctx.getCurrentKey() + "在2s内连续登陆失败达到2次，可能是恶意登陆！！！");
                                }
                                failEvents.clear();
                                timerTs.clear();
                            }
                        }
                )
                .print();


        env.execute();
    }
}
