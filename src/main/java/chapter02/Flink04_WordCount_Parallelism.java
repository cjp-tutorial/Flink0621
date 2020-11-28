package chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
public class Flink04_WordCount_Parallelism {
    public static void main(String[] args) throws Exception {
        //TODO 并行度优先级
        // 算子（代码） > 执行环境（代码） > 提交参数 > 配置文件

        // TODO 并行度与slot的关系
        // slot数量 >= 算子的最大并行度

        // task：是由 不同算子的 subtask 串在一起组成
        // subtask：某一个算子的 一个 并行 任务
        // 整个job的并行度，由 并行度最大的 算子 决定

        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);

        env.disableOperatorChaining();

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
//                .startNewChain(); // 以当前算子为起点，开启一个新的链条
//                .disableChaining(); // 当前算子禁用操作链，不加入任何链条（仅作用于当前算子）

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = wordAndOneDS.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        });

        // 3.2 按照word分组
        KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = mapDS.keyBy(0);
        // 3.3 求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        // 4.输出
        resultDS.print();

        // 5.执行
        env.execute();
    }
}
