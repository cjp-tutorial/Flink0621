package chapter06;

import bean.ItemCountWithWindowEnd;
import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/4 16:14
 */
public class Flink30_Case_HotItemWithWindowAnalysis {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000L)
                );

        // 3.处理数据
        // 3.1 过滤出pv行为
        SingleOutputStreamOperator<UserBehavior> filterDS = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        // 3.2 按照 统计维度（商品） 分组
        KeyedStream<UserBehavior, Long> userBehaviorKS = filterDS.keyBy(r -> r.getItemId());
        // 3.3 按照要求开窗: 每5分钟输出最近1小时 => 滑动窗口，长度 1小时， 步长 5分钟
        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaviorKS.timeWindow(Time.hours(1), Time.seconds(5));
        // 3.4 统计、排序、取前 N个
        // 转成 （商品ID，1），用 sum求和 => 怎么排序？
        // reduce？ => 类型限制， 即使可以求和，怎么排序？
        // aggregate ？ => 类型灵活， 求和完同样没了窗口信息， 怎么窗口内排序？
        // TODO aggregate 传两个参数
        // 第一个参数： 增量聚合，来一条算一条，最终得到一个聚合结果，这个结果会作为 第二个参数 的 输入
        //  1001
        //  1001
        //  1001
        //  1001
        //  1001
        //  1001
        //  1001
        //  1002
        //  1002
        //  1002
        //  1002
        //  1002
        //  => 每个分组，来一条，聚合一条，最终得到 1001分组的结果 7， 1002分组的结果 5
        //  => 将 7 和 5 这两个结果数据，传递给 第二个参数（全窗口函数）
        // 第二个参数： 全窗口函数， 它的输入是 第一个参数的 输出
        //    => 目的：打上 窗口结束时间 的标签 => 为了后续，根据标签进行分组 => 为了让 同一个窗口的 统计结果 在一起，后续进行排序
        SingleOutputStreamOperator<ItemCountWithWindowEnd> aggDS = userBehaviorWS.aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

        // 3.5 根据 窗口结束时间 分组 => 让 同一个窗口的 统计结果 在一起
        KeyedStream<ItemCountWithWindowEnd, Long> aggKS = aggDS.keyBy(r -> r.getWindowEnd());
        // 3.6 使用 process 实现排序
        aggKS
                .process(new TopNFunction(3))
                .print();

        // 4.输出


        env.execute();
    }

    /**
     * 第一个参数： 增量聚合函数
     * 它的聚合结果 会 传递给 第二个参数作为输入
     */
    public static class MyAggregateFunction implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    /**
     * 第二个参数：打上 窗口结束时间 的标签 => 为了后续，根据标签进行分组 => 为了让 同一个窗口的 统计结果 在一起，后续进行排序
     */
    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, ItemCountWithWindowEnd, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<ItemCountWithWindowEnd> out) throws Exception {
            out.collect(new ItemCountWithWindowEnd(key, elements.iterator().next(), context.window().getEnd()));
        }
    }


    public static class TopNFunction extends KeyedProcessFunction<Long, ItemCountWithWindowEnd, String> {
        ListState<ItemCountWithWindowEnd> datas;
        ValueState<Long> timerTs;
        // 定义一个属性，用来传参赋值
        private int threshold;

        public TopNFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<ItemCountWithWindowEnd>("datas", ItemCountWithWindowEnd.class));
//            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class,0L));
            timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(ItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 排序？ 数据是一条一条处理的，排序需要所有数据都在，所以 => 先存起来
            datas.add(value);
            // 存到啥时候？ => 模拟窗口的触发，可以考虑定时器 => 注册一个 比窗口结束时间 稍晚一点的定时器，数据肯定都存齐了
            if (timerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
                timerTs.update(value.getWindowEnd() + 1L);
            }
        }

        /**
         * 定时器触发，说明存齐了，该排序了
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 取出数据
            Iterable<ItemCountWithWindowEnd> itemCountWithWindowEnds = datas.get();
            // 转成 List， 进行排序
            ArrayList<ItemCountWithWindowEnd> dataList = new ArrayList<>();
            for (ItemCountWithWindowEnd itemCountWithWindowEnd : itemCountWithWindowEnds) {
                dataList.add(itemCountWithWindowEnd);
            }
            // 过河拆桥
            datas.clear();
            timerTs.clear();
            // 排序
            dataList.sort(
                    new Comparator<ItemCountWithWindowEnd>() {
                        @Override
                        public int compare(ItemCountWithWindowEnd o1, ItemCountWithWindowEnd o2) {
                            // 降序： 后 减 前 降序
                            // 升序： 前 减 后 升序
                            return (int) (o2.getItemCount() - o1.getItemCount());
                        }
                    }
            );

            // 取 前 N 个
            StringBuffer resultStr = new StringBuffer();
            resultStr.append("==============================================\n");
            for (int i = 0; i < ((threshold <= dataList.size()) ? threshold : dataList.size()); i++) {
                resultStr.append("Top" + (i + 1) + ":" + dataList.get(i) + "\n");
            }
            resultStr.append("=======================================\n\n\n");

            out.collect(resultStr.toString());
        }
    }
}
