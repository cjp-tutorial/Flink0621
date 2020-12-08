package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
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
public class Flink05_SQL_DDL {
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

        // TODO SQL基本使用
        // TODO 1.创建 表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.将 流 转换成 动态表
        // 还可以一步到位，直接 把DataStream 变成 Table，并且起一个名字
        tableEnv.createTemporaryView("sensor", sensorDS, $("id"), $("ts"), $("vc"));

//         TODO 3.使用 SQL 对 动态表 进行操作,返回一个 结果表
        Table resultTable = tableEnv
                .sqlQuery("select * from sensor where id='sensor_1'");
        tableEnv.createTemporaryView("result", resultTable);

        // TODO 4.使用 SQL 将 外部系统 抽象成 Table
        tableEnv.executeSql("create table fsTable (a String,b bigint,c int) " +
                "with (" +
                "'connector'='filesystem'," +
                "'path'='output'," +
                "'format'='csv'" +
                ")");

        // TODO 5.对其进行写入，相当于 sink到外部系统
        // 关键字Bug：result =》 1.要么避免和关键字重复， 2、 要么 加 ``
        tableEnv.executeSql("insert into fsTable select * from `result`");

        env.execute();
    }
}
