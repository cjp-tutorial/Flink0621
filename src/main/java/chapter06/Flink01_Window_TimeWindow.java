package chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink01_Window_TimeWindow {
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

        //TODO 开窗
//        socketDS.windowAll()
//        socketDS.timeWindowAll()
//        socketDS.countWindowAll()

        KeyedStream<Tuple2<String, Integer>, String> socketKS = socketDS.keyBy(r -> r.f0);


        //TODO Flink窗口的划分，不是按照第一条数据进入为起点
        // 而是有一定的算法进行划分

        socketKS
//                .timeWindow(Time.seconds(5))    // 滚动窗口
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .timeWindow(Time.seconds(5),Time.seconds(2))    // 滑动窗口： 第一个参数 窗口长度，第二个参数 滑动步长
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))  // 会话窗口，参数是 时间间隔
                .sum(1)
                .print();


        env.execute();
    }
}
