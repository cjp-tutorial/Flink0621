package chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink08_Transform_Filter {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4, 5);

        // TODO Filter
        numDS
//                .filter(new FilterFunction<Integer>() {
//                    @Override
//                    public boolean filter(Integer value) throws Exception {
//                        return value % 2 == 0;
//                    }
//                })
                .filter(data -> data % 2 == 0)
                .print();


        env.execute();
    }


}
