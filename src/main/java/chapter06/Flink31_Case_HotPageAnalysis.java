package chapter06;

import bean.ApacheLog;
import bean.PageCountWithWindowEnd;
import bean.SimpleAggregateFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/5 9:16
 */
public class Flink31_Case_HotPageAnalysis {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // todo ck、状态后端....

        // 2.读取数据
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env
                .readTextFile("input/apache.log")
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String value) throws Exception {
                        String[] datas = value.split(" ");
                        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        Date date = sdf.parse(datas[3]);
                        long eventTime = date.getTime();
                        return new ApacheLog(
                                datas[0],
                                datas[2],
                                eventTime,
                                datas[5],
                                datas[6]);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ApacheLog>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((log, ts) -> log.getEventTime())
                );

        // 3. 处理数据
        // 3.1 按照 统计维度(url) 分组、开窗、聚合、打标签
        SingleOutputStreamOperator<PageCountWithWindowEnd> logAggDS = apacheLogDS
                .keyBy(r -> r.getUrl())
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(
                        new SimpleAggregateFunction<ApacheLog>(),
                        new ProcessWindowFunction<Long, PageCountWithWindowEnd, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Long> elements, Collector<PageCountWithWindowEnd> out) throws Exception {
                                out.collect(new PageCountWithWindowEnd(s, elements.iterator().next(), context.window().getEnd()));
                            }
                        });

        // 3.2 按照 windowEnd 分组，排序
        logAggDS
                .keyBy(agg -> agg.getWindowEnd())
                .process(
                        new KeyedProcessFunction<Long, PageCountWithWindowEnd, String>() {
                            ListState<PageCountWithWindowEnd> datas;
                            ValueState<Long> timerTs;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                datas = getRuntimeContext().getListState(new ListStateDescriptor<PageCountWithWindowEnd>("page-count-windowend", PageCountWithWindowEnd.class));
                                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                            }

                            @Override
                            public void processElement(PageCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
                                // 存
                                datas.add(value);
                                // 注册定时器
                                if (timerTs.value() == null) {
                                    ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                                    timerTs.update(value.getWindowEnd() + 10L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                List<PageCountWithWindowEnd> pageCountWithWindowEnds = new ArrayList<>();
                                for (PageCountWithWindowEnd pageCountWithWindowEnd : datas.get()) {
                                    pageCountWithWindowEnds.add(pageCountWithWindowEnd);
                                }
                                // 杀
                                datas.clear();
                                timerTs.clear();
                                // 排序
                                Collections.sort(pageCountWithWindowEnds);
                                // 取前N
                                // 取 前 N 个
                                StringBuffer resultStr = new StringBuffer();
                                resultStr.append("==============================================\n");
                                for (int i = 0; i < Math.min(3, pageCountWithWindowEnds.size()); i++) {
                                    resultStr.append("Top" + (i + 1) + ":" + pageCountWithWindowEnds.get(i) + "\n");
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
