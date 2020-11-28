package chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 10:10
 */
public class Flink02_Source_example {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Source
        DataStreamSource<String> fileDS = env.readTextFile("input/word.txt");
        fileDS.print("file");
        DataStreamSource<String> collectionDS = env.fromCollection(Arrays.asList("1", "2", "3"));
        collectionDS.print("collection");
        DataStreamSource<Integer> elementDS = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        elementDS.print("element");
//        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);
//        socketDS.print();




        env.execute("source job");
    }
}
