package chapter06;

import bean.AdClickLog;
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
public class Flink34_Case_BlacklistFIlter {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2.读取数据
        SingleOutputStreamOperator<AdClickLog> adClickDS = env
                .readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<AdClickLog>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000L)
                );
        // 3.处理数据
        // 3.1 按照 统计维度 分组： 用户、广告
        KeyedStream<AdClickLog, Tuple2<Long, Long>> adClickKS = adClickDS.keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getUserId(), value.getAdId());
            }
        });
        // 3.2 使用 process 实现 黑名单过滤
        SingleOutputStreamOperator<AdClickLog> blackFilter = adClickKS
                .process(new BlacklistFilterFunction());

        OutputTag<String> outputTag = new OutputTag<String>("blacklist") {
        };
        blackFilter.getSideOutput(outputTag).print("blacklist");
        // 3.3 对过滤完的 数据 进行其他正常的业务分析
        // 需要重新 keyby
        blackFilter
                .keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                        return Tuple2.of(value.getUserId(), value.getAdId());
                    }
                })
                .timeWindow(Time.hours(1), Time.minutes(5))
                .allowedLateness(Time.seconds(6))
                .aggregate(new SimpleAggregateFunction<AdClickLog>(),
                        new ProcessWindowFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple2<Long, Long>, TimeWindow>() {
                            @Override
                            public void process(Tuple2<Long, Long> stringLongTuple2, Context context, Iterable<Long> elements, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
                                // (key1,key2,count,windowEnd)
                                out.collect(Tuple4.of(stringLongTuple2.f0, stringLongTuple2.f1, elements.iterator().next(), context.window().getEnd()));
                            }
                        })
                .keyBy(r -> r.f3)
                .process(
                        new KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, String>() {
                            ListState<Tuple4<Long, Long, Long, Long>> datas;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                datas = getRuntimeContext().getListState(new ListStateDescriptor<Tuple4<Long, Long, Long, Long>>("datas",
                                        TypeInformation.of(new TypeHint<Tuple4<Long, Long, Long, Long>>() {
                                        })));

                            }

                            @Override
                            public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
                                // 存数据
                                datas.add(value);
                                // 注册定时器
                                ctx.timerService().registerEventTimeTimer(value.f3 + 100L);
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 取出数据、排序
                                List<Tuple4<Long, Long, Long, Long>> list = new ArrayList<>();
                                for (Tuple4<Long, Long, Long, Long> data : datas.get()) {
                                    list.add(data);
                                }
                                // 清空状态
                                datas.clear();
                                // 排序
                                list.sort(new Comparator<Tuple4<Long, Long, Long, Long>>() {
                                    @Override
                                    public int compare(Tuple4<Long, Long, Long, Long> o1, Tuple4<Long, Long, Long, Long> o2) {
                                        return (int) (o2.f2 - o1.f2);
                                    }
                                });

                                // 取前N
                                // 取 前 N 个
                                StringBuffer resultStr = new StringBuffer();
                                resultStr.append("==============================================\n");
                                for (int i = 0; i < Math.min(3, list.size()); i++) {
                                    resultStr.append("Top" + (i + 1) + ":" + list.get(i) + "\n");
                                }
                                resultStr.append("=======================================\n\n\n");

                                out.collect(resultStr.toString());
                            }
                        }
                )
                .print("topN");


        env.execute();
    }

    public static class BlacklistFilterFunction extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {

        ValueState<Integer> clickcount;
        ValueState<Long> timerTs;
        ValueState<Boolean> isAlarm;

        @Override
        public void open(Configuration parameters) throws Exception {
            clickcount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("clickCount", Integer.class, 0));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
            isAlarm = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isAlarm", Boolean.class, false));
        }

        @Override
        public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out) throws Exception {

            // 考虑 隔天 0 点 清空 统计值 => 定时器 => 怎么获取隔天0点的时间戳
            if (timerTs.value() == null) {
                // 说明还没注册过定时器 => 当天的第一条数据 => 1.获取隔天0点 => 2.注册 隔天0点的定时器
                long untilNowDays = ctx.timestamp() / (24 * 60 * 60 * 1000L); // 从 1970年到现在，经过了多少天
                long untilNextdayDays = untilNowDays + 1; // 从 1970年到明天，经过了多少天
                long nextDayTs = untilNextdayDays * (24 * 60 * 60 * 1000L); // 从 1970年到隔天0点，经过了多少时间戳
                timerTs.update(nextDayTs);
                ctx.timerService().registerEventTimeTimer(nextDayTs);
            }


            Integer currentCount = clickcount.value();
            // 1.判断统计次数是否 达到100 => 计数 => 定义一个 count（需要分组隔离，也就是 哪个 用户 对 哪个广告 的点击分开） => 键控状态
            if (currentCount >= 100) {
                // 达到阈值，应该告警 => 首次达到阈值告警一次就行了，后续的不再告警 => 加一个 标志位 判断

                if (!isAlarm.value()) { // 为 false表示没告过警，又想要进入if里面的逻辑，加一个 非！
                    OutputTag<String> outputTag = new OutputTag<String>("blacklist") {
                    };
                    ctx.output(outputTag, "用户" + value.getUserId() + "对广告" + value.getAdId() + "点击次数达到阈值，可能存在刷单行为！！！");
                    isAlarm.update(true);
                }
            } else {
                // 如果没达到阈值，才继续进行 点击次数的 统计
                clickcount.update(currentCount + 1);
                // 没达到阈值，就是正常的，往后传递，就类似 filter里面为true，过滤之后剩下的
                out.collect(value);
            }

        }

        /**
         * 定时器触发，说明到了隔天0点，要清除统计值
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
            //
            clickcount.clear();
            timerTs.clear();
            isAlarm.clear();
        }
    }
}
