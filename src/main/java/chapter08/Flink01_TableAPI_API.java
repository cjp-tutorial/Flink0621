package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/8 9:21
 */
public class Flink01_TableAPI_API {
    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.获取流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                                .withTimestampAssigner((sensor, recordTs) -> sensor.getTs() * 1000L)
                );

        // TODO TableAPI基本使用
        // TODO 1.创建 表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2.将 流 转换成 动态表
        Table sensorTable = tableEnv.fromDataStream(sensorDS, $("ts"), $("id"), $("vc").as("ergou"));
//        Table sensorTable = tableEnv.fromDataStream(sensorDS,"id,ts,vc");
        // TODO 3.使用 TableAPI对 动态表 进行操作,返回一个 结果表
        Table resultTable = sensorTable
//                .where("id = 'sensor_1'")
                .where($("id").isEqual("sensor_1"))
//                .select("id,ts,vc")
                .select($("id"), $("ergou"));
        // TODO 4.将 动态表 转换成 流，输出
//        DataStream<Row> resultDS = tableEnv.toAppendStream(resultTable, Row.class);
        DataStream<Tuple2<String, Integer>> resultDS = tableEnv.toAppendStream(resultTable, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        resultDS.print();

        env.execute();
    }
}
