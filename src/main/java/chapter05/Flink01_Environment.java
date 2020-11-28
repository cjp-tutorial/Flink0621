package chapter05;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 9:36
 */
public class Flink01_Environment {
    public static void main(String[] args) {
        // 批处理的 执行环境
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        // 流处理的 执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

//        StreamExecutionEnvironment.createLocalEnvironment();
//                StreamExecutionEnvironment.createRemoteEnvironment(, );
    }
}
