package chapter06;

import bean.AdClickLog;
import bean.CountByProAdWithWindowEnd;
import bean.SimpleAggregateFunction;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
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

import java.io.Serializable;
import java.sql.Blob;
import java.sql.Clob;
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
public class Flink32_HotAdClickAnalysis {
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
        // 3.1 按照 统计维度 分组： 省份、广告
        KeyedStream<AdClickLog, Tuple2<String, Long>> adClickKS = adClickDS.keyBy(new KeySelector<AdClickLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdClickLog value) throws Exception {
                return Tuple2.of(value.getProvince(), value.getAdId());
            }
        });
        // 3.2 开窗
        OutputTag<AdClickLog> outputTag = new OutputTag<AdClickLog>("late-late") {
        };
        adClickKS
                .timeWindow(Time.hours(1), Time.minutes(5))
                .allowedLateness(Time.seconds(6))
                .sideOutputLateData(outputTag)
                .aggregate(new SimpleAggregateFunction<AdClickLog>(),
                        new ProcessWindowFunction<Long, CountByProAdWithWindowEnd, Tuple2<String, Long>, TimeWindow>() {
                            @Override
                            public void process(Tuple2<String, Long> stringLongTuple2, Context context, Iterable<Long> elements, Collector<CountByProAdWithWindowEnd> out) throws Exception {
                                out.collect(new CountByProAdWithWindowEnd(stringLongTuple2.f0, stringLongTuple2.f1, elements.iterator().next(), context.window().getEnd()));
                            }
                        })
                .keyBy(r -> r.getWindowEnd())
                .process(
                        new KeyedProcessFunction<Long, CountByProAdWithWindowEnd, String>() {
                            ListState<CountByProAdWithWindowEnd> datas;
                            ListState<Tuple8<Integer, Long, Double, Blob, Clob, Byte, String, Serializable>> aaa;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                datas = getRuntimeContext().getListState(new ListStateDescriptor<CountByProAdWithWindowEnd>("datas", CountByProAdWithWindowEnd.class));
                                // TypeInformation.of(new TypeHint<T>(){})
/*                                aaa = getRuntimeContext().getListState(new ListStateDescriptor<Tuple8<Integer, Long, Double, Blob, Clob, Byte, String, Serializable>>("aaa",
                                        TypeInformation.of(new TypeHint<Tuple8<Integer, Long, Double, Blob, Clob, Byte, String, Serializable>>() {
                                        })));*/
                            }

                            @Override
                            public void processElement(CountByProAdWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
                                // 存数据
                                datas.add(value);
                                // 注册定时器
                                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 取出数据、排序
                                List<CountByProAdWithWindowEnd> list = new ArrayList<>();
                                for (CountByProAdWithWindowEnd countByProAdWithWindowEnd : datas.get()) {
                                    list.add(countByProAdWithWindowEnd);
                                }
                                // 清空状态
                                datas.clear();
                                // 排序
                                list.sort(new Comparator<CountByProAdWithWindowEnd>() {
                                    @Override
                                    public int compare(CountByProAdWithWindowEnd o1, CountByProAdWithWindowEnd o2) {
                                        return (int) (o2.getCount() - o1.getCount());
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
                .print();

        env.execute();
    }
}
