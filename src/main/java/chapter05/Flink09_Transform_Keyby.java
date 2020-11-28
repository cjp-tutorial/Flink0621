package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink09_Transform_Keyby {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        // TODO Keyby
        // keyby对数据进行 分组 =》 同一组的数据 在一起
        // keyby是一个逻辑上的分组，跟资源没有强绑定

        // TODO 源码简介
        // key做了两次hash： 第一次，自己调用hashcode方法，第二次 mermerhash
        // 两次hash之后，对 默认值128 取模，得到一个 ID值
        // ID值 * 并行度 / 默认值128 得到selectChannel的 Channel值

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> sensorTupleDS = sensorDS.map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                return Tuple3.of(value.getId(), value.getTs(), value.getVc());
            }
        });

        // 使用位置索引的方式，只能确定位置，程序并无法确定类型，所以统一给个Tuple
//        KeyedStream<Tuple3<String, Long, Integer>, Tuple> sensorKS = sensorTupleDS.keyBy(0);
        // 使用 属性名 的方式，只能确定名字，程序并无法确定类型，所以统一给个Tuple
//        KeyedStream<WaterSensor, Tuple> sensorKS = sensorDS.keyBy("id");


//        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
//            // 从 数据里 提取（指定） key
//            @Override
//            public String getKey(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        sensorDS.print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Integer, String> {

        @Override
        public String map(Integer value) throws Exception {
            return String.valueOf(value * 2) + "===================";
        }
    }
}
