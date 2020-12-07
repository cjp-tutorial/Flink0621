package chapter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/7 11:37
 */
public class Flink06_Join_WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> ds1 = env.fromElements(
                Tuple2.of("a", 111000L),
                Tuple2.of("b", 112000L),
                Tuple2.of("a", 113000L),
                Tuple2.of("c", 114000L),
                Tuple2.of("d", 115000L)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((data, ts) -> data.f1)
                );

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> ds2 = env.fromElements(
                Tuple3.of("a", 111000L, 1),
                Tuple3.of("a", 112000L, 11),
                Tuple3.of("a", 113000L, 111),
                Tuple3.of("b", 114000L, 1111),
                Tuple3.of("e", 115000L, 11111),
                Tuple3.of("d", 116000L, 111111),
                Tuple3.of("c", 117000L, 1111111)
        )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((data, ts) -> data.f1)
                );


        //TODO Window Join
        ds1.join(ds2)
                .where(a -> a.f0)
                .equalTo(b -> b.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(
                        new JoinFunction<Tuple2<String, Long>, Tuple3<String, Long, Integer>, String>() {
                            @Override
                            public String join(Tuple2<String, Long> first, Tuple3<String, Long, Integer> second) throws Exception {
                                // 说明进入join方法的，是 join上的数据
                                return first + " <----> " + second;
                            }
                        })
                .print();

        env.execute();
    }
}
