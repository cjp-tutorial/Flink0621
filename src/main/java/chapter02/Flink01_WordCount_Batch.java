package chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * wordcount 批处理
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/27 9:33
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        // 1.初始化执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取数据
        DataSource<String> fileDS = env.readTextFile("input/word.txt");

        // 3.处理数据
        // 3.1 压平：切分、转换成kv形式
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneDS = fileDS.flatMap(new MyFlatmapFunction());
        // 3.2 按照 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOneDS.groupBy(0);
        // 3.3 聚合
        AggregateOperator<Tuple2<String, Integer>> resultDS = wordAndOneGroup.sum(1);

        // 4.输出结果
        resultDS.print();

        // 5.执行(批处理不用)
    }

    public static class MyFlatmapFunction implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 1.切分
            String[] words = value.split(" ");
            for (String word : words) {
                // 2.转换成元组
                Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                // 3.通过采集器，往下游传输
                out.collect(tuple2);
            }
        }
    }
}
