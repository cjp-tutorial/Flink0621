package chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink03_Window_IncreAgg {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> socketDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                });


        KeyedStream<Tuple2<String, Integer>, String> socketKS = socketDS.keyBy(r -> r.f0);


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> socketWS = socketKS.timeWindow(Time.seconds(10));


        // 窗口的增量聚合函数：
        // 窗口内，同一组的数据，来一条 算一条， 窗口关闭的时候 才会输出一次结果
        socketWS
//                .reduce(
//                        new ReduceFunction<Tuple2<String, Integer>>() {
//                            @Override
//                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                                System.out.println(value1 + "<----->" + value2);
//                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                            }
//                        }
//                )
                // 窗口内 同一组的数据， 来一条，算一次，最后窗口关闭的时候，输出一次
                // 输入的类型、输出的类型、累加器的类型，可以不一样，更灵活一点
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        System.out.println("create....");
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Integer> value, Long accumulator) {
                        System.out.println("add...");
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("getResult...");
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("merge...");
                        return a+b;
                    }
                })
                .print();

        env.execute();
    }
}
