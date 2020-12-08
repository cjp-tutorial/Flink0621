package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
public class Flink06_SQL_API {
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

        SingleOutputStreamOperator<WaterSensor> sensorDS1 = env
                .readTextFile("input/sensor-data-cep.log")
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
        tableEnv.createTemporaryView("sensor", sensorDS, $("id"), $("ts"), $("vc"));
        tableEnv.createTemporaryView("sensor1", sensorDS1, $("id"), $("ts"), $("vc"));

        // TODO 3.使用 SQL 对 动态表 进行操作,返回一个 结果表
        Table resultTable = tableEnv
                .sqlQuery("select * from sensor where id='sensor_1'"); // 条件查询
//                .sqlQuery("select id,count(id) cnt from sensor group by id"); // 分组查询
//                .sqlQuery("select * from sensor right join sensor1 on sensor.id=sensor1.id"); // 分组查询
//                .sqlQuery("select * from sensor where id not in (select id from sensor1)"); // 范围查询
//                .sqlQuery("select (case id when 'sensor_1' then 1 else 0 end) test from sensor"); // case when查询

        // TODO 4.将 动态表 转换成 流，输出
        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(resultTable, Row.class);

        resultDS.print();

        env.execute();
    }
}
