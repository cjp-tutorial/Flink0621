package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink11_Transform_Split {
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

        //TODO Split & Select
        // 逻辑上作 切分 ，打上标签，实际上还在一条流里面
        // 要用的使用，通过 select（标签名） 取出特定的数据
        SplitStream<WaterSensor> sensorSS = sensorDS.split(new OutputSelector<WaterSensor>() {
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 5) {
                    return Arrays.asList("low","hahaha");
                } else if (value.getVc() < 8) {
                    return Arrays.asList("middle","hahaha");
                } else {
                    return Arrays.asList("high");
                }
            }
        });


        sensorSS.select("hahaha").print();


        env.execute();
    }

}
