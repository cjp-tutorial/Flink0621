package chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink04_Window_ProcessWindowFunction {
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

        // TODO 全窗口函数
        // 先存数据，等到窗口触发时，才一起计算
        socketWS
                .process(
                        new ProcessWindowFunction<Tuple2<String, Integer>, Long, String, TimeWindow>() {
                            /**
                             * 窗口内 同一分组 的 所有数据
                             * @param key 分组
                             * @param context   上下文
                             * @param elements  窗口内 同一分组 所有数据
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Long> out) throws Exception {
                                System.out.println("process...");
                                long count = elements.spliterator().estimateSize();
                                out.collect(count);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
