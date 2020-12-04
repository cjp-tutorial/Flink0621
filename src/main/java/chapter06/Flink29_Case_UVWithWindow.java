package chapter06;

import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.text.SimpleDateFormat;
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
public class Flink29_Case_UVWithWindow {
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

        WindowedStream<Tuple2<String, Long>, String, TimeWindow> uvWS = uvKS.timeWindow(Time.hours(1));
        // 3.4 把 userId添加到 Set里
        uvWS
                .process(
                        new ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
                            // 创建一个Set，用来存放 userId
                            Set<Long> uvSet = new HashSet<>();

                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                                uvSet.clear();
                                for (Tuple2<String, Long> element : elements) {
                                    // 把userId添加到 Set里
                                    uvSet.add(element.f1);
                                }
                                out.collect(Long.valueOf(uvSet.size()));
                            }
                        }
                )
                .print();

        env.execute();
    }
}
