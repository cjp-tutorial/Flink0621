package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink25_Case_UV {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                });

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
        // 3.4 把 userId添加到 Set里
        SingleOutputStreamOperator<Long> uvCount = uvKS.process(
                new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {
                    // 定义一个Set，用来 存放 userId，实现去重
                    Set<Long> uvSet = new HashSet();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                        // 取出 userId
                        Long userId = value.f1;
                        uvSet.add(userId);
                        out.collect(Long.valueOf(uvSet.size()));
                    }
                }
        );

        // 4. 输出
        uvCount.print();

        env.execute();
    }
}
