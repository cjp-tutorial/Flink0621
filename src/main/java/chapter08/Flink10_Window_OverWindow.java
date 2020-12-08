package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_ROW;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_ROW;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/8 9:21
 */
public class Flink10_Window_OverWindow {
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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.createTemporaryView("sensor", sensorDS, $("id"), $("ts").rowtime(), $("vc"));
        Table sensorTable = tableEnv.from("sensor");

        // TODO 3.使用Table API 对 Table 开 OverWindow
/*        Table resultTable = sensorTable
                .window(
                        Over
                                .partitionBy($("id"))
                                .orderBy($("ts").desc())
                                .preceding(UNBOUNDED_ROW)
                                .following(CURRENT_ROW)
                                .as("ow")
                )
                .select($("*"), $("id").count().over($("ow")));*/

        // TODO 3. 使用 SQL 对 Table 开 OverWindow
        Table resultTable = tableEnv.sqlQuery(
                "select *," +
                        "count(id) over(partition by id order by ts desc) as cow\n" +
                        "from sensor"
        );

        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
