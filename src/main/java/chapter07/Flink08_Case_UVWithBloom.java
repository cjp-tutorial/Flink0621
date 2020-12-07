package chapter07;

import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink08_Case_UVWithBloom {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读数据
        SingleOutputStreamOperator<UserBehavior> userbehaviorDS = env
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
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000L)
                );

        // 3.处理数据
        // 3.1 能过滤就先过滤
        SingleOutputStreamOperator<UserBehavior> pvDS = userbehaviorDS.filter(sensor -> "pv".equals(sensor.getBehavior()));

        // 对 userId 进行去重
        // 3.2 转换成 （uv，userId）
        // => 第一个元素，给个固定的字符串 “uv” => 用来做 keyby
        // => 第二个元素，是 用户ID， 用来 添加到 Set里，进行去重， 后续可以用  Set.size 得到 uv值
        SingleOutputStreamOperator<Tuple2<String, Long>> uvDS = pvDS.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });
        // 3.3 按照 "uv" 分组
        KeyedStream<Tuple2<String, Long>, String> uvKS = uvDS.keyBy(r -> r.f0);

        // TODO 问题分析：
        // 1. 全窗口函数，存 10亿条数据，撑不住
        // 2. Set存用户ID，
        //      =》一个 long 8字节， 10亿 * 8字节 = 2^3 * 10^9 B ≈ 8G
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> uvWS = uvKS.timeWindow(Time.hours(1));
        // TODO 3.4 使用 布隆过滤器 去重，并统计
        uvWS.aggregate(new UvCountByBloomFilter(), new MyProcessWindowFunction())
                .print();


        env.execute();
    }

    public static class UvCountByBloomFilter implements AggregateFunction<Tuple2<String, Long>, Tuple2<BloomFilter<Long>, Long>, Long> {

        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            // 1.第一个参数： 指定数据类型
            // 2.期望的数据量
            // 3.期望的误判率
            BloomFilter<Long> bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01D);
            // 第一个参数，布隆过滤器
            // 第二个参数，count值 =》 经过布隆，发现不存在，就 +1，发现存在，就不统计
            return Tuple2.of(bloomFilter, 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Tuple2<String, Long> value, Tuple2<BloomFilter<Long>, Long> accumulator) {
            // 取出对应的东西
            Long userId = value.f1;
            BloomFilter<Long> bloomFilter = accumulator.f0;
            Long uvCount = accumulator.f1;
            // 1.查 布隆
            if (!bloomFilter.mightContain(userId)) {
                // 2.布隆 返回 false，说明 不存在了 =》uv统计值 + 1
                uvCount++;
                // 3.把布隆 设置为 来过
                bloomFilter.put(userId);
            }
            return Tuple2.of(bloomFilter, uvCount);
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口为[" + context.window().getStart() + "," + context.window().getEnd() + "),uv值=" + elements.iterator().next());
        }
    }
}
