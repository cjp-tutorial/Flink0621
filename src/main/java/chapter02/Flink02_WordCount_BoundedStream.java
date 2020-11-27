package chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * wordcount - 有界流处理（有头有尾） - 文件
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/27 10:13
 */
public class Flink02_WordCount_BoundedStream {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.读数据
//        DataStreamSource<String> inputDS = env.readTextFile("input/word.txt");
        DataStreamSource<String> inputDS = env.readTextFile("/opt/module/data/word.txt"); // 虚拟机的文件路径

        // 3.处理数据
        // 3.1 压平操作：切分、转换成（word，1）形式
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 切分
                String[] words = value.split(" ");
                for (String word : words) {
                    // 转换成（word，1），使用采集器往下游发送
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 3.2 按照 word 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneDS.keyBy(0);
        // 3.3 组内求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 4.输出
        resultDS.print();

        // 5.执行
        env.execute();
    }
}
