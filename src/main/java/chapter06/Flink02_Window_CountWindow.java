package chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink02_Window_CountWindow {
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


        // 每经过一个步长，有一个窗口触发
        socketKS
//                .countWindow(3)
                .countWindow(5,2)
                .sum(1)
                .print();


        env.execute();
    }
}
