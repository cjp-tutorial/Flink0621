package chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * wordcount - 无界流（有头无尾）：kafka、socket
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/27 10:24
 */
public class Flink03_WordCount_UnBoundedStream {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);
//        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        // 3.处理数据
        // 3.1 压平：切分、转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = socketDS
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                    @Override
                    public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                });

        // 3.2 按照word分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneDS.keyBy(0);
        // 3.3 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 4.输出
        resultDS.print();

        // 5.执行
        env.execute();
    }
}
