package chapter07;

import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.Duration;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink09_Case_UVWithBloomByRedis {
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
        uvKS
                .timeWindow(Time.hours(1))
                .trigger(
                        new Trigger<Tuple2<String, Long>, TimeWindow>() {

                            /**
                             * 来一条数据，怎么从处理
                             *
                             * @param element
                             * @param timestamp
                             * @param window
                             * @param ctx
                             * @return
                             * @throws Exception
                             */
                            @Override
                            public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                                // 来一条数据，就触发计算，并且不保存数据
                                return TriggerResult.FIRE_AND_PURGE;
                            }

                            /**
                             * 达到处理时间，怎么处理
                             *
                             * @param time
                             * @param window
                             * @param ctx
                             * @return
                             * @throws Exception
                             */
                            @Override
                            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            /**
                             * 达到事件时间，怎么处理
                             *
                             * @param time
                             * @param window
                             * @param ctx
                             * @return
                             * @throws Exception
                             */
                            @Override
                            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                                return TriggerResult.CONTINUE;
                            }

                            @Override
                            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                            }
                        }
                )
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                            Jedis jedis;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                jedis = new Jedis("hadoop", 6379);
                            }

                            @Override
                            public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                                // 因为改了触发器，现在 elements只有一条数据，而且 每来一条数据，执行一次process方法
                                // TODO redis存储方案 设计：1.存 每个窗口对应的 位图； 2.存 每个窗口对应的 count值
                                // redis=》bitmap结构 => (key,bitmap) =》bitmap多少位?, 根据字符串key的长度得到
                                // key给什么？ =》 （windowEnd，bitmap），一个窗口对应一个 bitmap
                                // count值在redis里怎么存 => （"uvCount", ("windowEnd",count值)）

                                String userIdStr = String.valueOf(elements.iterator().next().f1);
                                String windowEndStr = String.valueOf(context.window().getEnd());
                                // 1.到redis的bitmap里查询是否存在
                                // 第一个参数：数据， 第二个参数：对应的 格子编号
                                MyBloomFilterHashFunction myBloomFilterHashFunction = new MyBloomFilterHashFunction(1 << 33, 61);
                                long offset = myBloomFilterHashFunction.getOffset(userIdStr);
                                Boolean isExist = jedis.getbit(windowEndStr, offset);


                                // 2.如果不存在，count值+1，同时把 布隆对应的格子 置为已存在
                                String uvCount = jedis.hget("uvCount", windowEndStr);
                                if (!isExist) {
                                    // 2.1 uv值 +1
                                    if (uvCount == null) {
                                        uvCount = "1";
                                    } else {
                                        uvCount = String.valueOf(Long.valueOf(uvCount) + 1L);
                                    }
                                    // 2.2 把 uv值 更新到 redis里
                                    jedis.hset("uvCount", windowEndStr, uvCount);
                                    // 2.3 把对应位图的格子置为 1
                                    jedis.setbit(windowEndStr, offset, true);
                                }

                            }

                            @Override
                            public void close() throws Exception {
                                jedis.close();
                            }
                        }
                )
                .print();
    }

    public static class MyBloomFilterHashFunction {

        private long hash = 0L;
        private long cap;
        // 随机数种子：最好是一个质数，可以有效避免hash碰撞
        private int seed;

        public MyBloomFilterHashFunction(long cap, int seed) {
            this.cap = cap;
            this.seed = seed;
        }

        public long getOffset(String userId) {
            for (char c : userId.toCharArray()) {
                hash = hash * seed + c;
            }
//            hash % cap
            // 参考 hashmap的实现， 把 取模 替换成 位运算, 要求是 cap 必须是 2的n次方
            long offset = hash & (cap - 1);
            return offset;
        }
    }
}
