package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink13_Transform_Union {
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

        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> numDS1 = env.fromElements(11, 12, 13, 14);
        DataStreamSource<Integer> numDS2 = env.fromElements(111, 112, 113, 114);

        ConnectedStreams<WaterSensor, Integer> sensorNumCS = sensorDS.connect(numDS);

        //TODO union
        // 1.可以 合并 多条流
        // 2.每条流 数据类型 必须一致
        DataStream<Integer> resultDS = numDS.union(numDS1).union(numDS2);

        resultDS.print();


        env.execute();
    }

}
