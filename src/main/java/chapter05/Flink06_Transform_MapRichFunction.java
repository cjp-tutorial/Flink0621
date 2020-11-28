package chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
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
public class Flink06_Transform_MapRichFunction {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4, 5);

        // 读文件 会有 特殊处理 ， 最后都会再调用一次 close
//        env.readTextFile("input/word.txt")
//                .map(new MyRichMapFunction())
//                .print();

        // TODO RichMapFunction 富函数
        // 1.生命周期方法：open、close =》可以用于 外部连接的 管理
        // 2.运行时上下文：RuntimeContext =》 可以获取 环境信息、状态....
        SingleOutputStreamOperator<String> resultDS = numDS.map(new MyRichMapFunction());

        resultDS.print();


        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Integer, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open....");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }


        @Override
        public String map(Integer value) throws Exception {
//            return String.valueOf(value * 2)+"===================";

            return value +"---------------"+ getRuntimeContext().getTaskNameWithSubtasks() ;
        }
    }
}
