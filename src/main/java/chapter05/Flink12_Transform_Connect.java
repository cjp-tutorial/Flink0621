package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink12_Transform_Connect {
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
                }).setParallelism(2);

        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4);

        ConnectedStreams<WaterSensor, Integer> sensorNumCS = sensorDS.connect(numDS);
        //TODO connect 连接两条流
        // 1.只能连接 两条流
        // 2.两条流的 数据类型 可以 不一样
        sensorNumCS
                .map(new CoMapFunction<WaterSensor, Integer, Object>() {
                    @Override
                    public Object map1(WaterSensor value) throws Exception {
                        return "我是WaterSensor" + value;
                    }

                    @Override
                    public Object map2(Integer value) throws Exception {
                        return "我是小x " + value;
                    }
                })
                .print("aaaaa");


        env.execute();
    }

}
