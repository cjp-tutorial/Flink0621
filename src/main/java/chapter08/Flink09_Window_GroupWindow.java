package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/8 9:21
 */
public class Flink09_Window_GroupWindow {
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

        // TODO 3.使用Table API 对 Table 开 GroupWindow
/*        Table resultTable = sensorTable
//                .window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
                .window(Slide.over(lit(3).seconds()).every(lit(2).seconds()).on($("ts")).as("w"))
                .groupBy($("w"), $("id"))
                .select($("id"), $("id").count().as("cnt"), $("w").start(), $("w").end());*/

        // TODO 3.使用 SQL 对 Table 开 GroupWindow
        Table resultTable = tableEnv.sqlQuery(
                "select id,count(id) cnt," +
                        "TUMBLE_START(ts,INTERVAL '3' SECOND) as window_start," +
                        "TUMBLE_END(ts,INTERVAL '3' SECOND) as window_end " +
                        "from sensor " +
                        "group by TUMBLE(ts,INTERVAL '3' SECOND),id"
        );


        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
