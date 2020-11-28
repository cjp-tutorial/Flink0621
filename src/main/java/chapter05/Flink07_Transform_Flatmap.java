package chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink07_Transform_Flatmap {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        // TODO flatmap 压平：一进多出、一进零出
        // 1. 可以实现类似过滤的效果， 不满足条件，就不用采集器往下游发送

        env
                .fromElements(
                        Arrays.asList(1, 2, 3, 4)
                )
                .flatMap(new FlatMapFunction<List<Integer>, String>() {
                    @Override
                    public void flatMap(List<Integer> value, Collector<String> out) throws Exception {
                        for (Integer num : value) {
                            if (num % 2 == 0) {
                                out.collect(num + "");
                            }
                        }
                    }
                })
                .print();


        env.execute();
    }


}
