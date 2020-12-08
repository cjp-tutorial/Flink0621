package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
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
public class Flink03_TableAPI_Connect {
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

        // TODO TableAPI基本使用: 将外部系统 抽象成 Table对象，进行 读写，就相当于 从外部系统 source、sink到外部系统
        // TODO 1.创建 表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2.将 流 转换成 动态表
        Table sensorTable = tableEnv.fromDataStream(sensorDS, $("ts"), $("id"), $("vc").as("ergou"));
//        Table sensorTable = tableEnv.fromDataStream(sensorDS,"id,ts,vc");
        // TODO 3.使用 TableAPI对 动态表 进行操作,返回一个 结果表
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("ergou"));

        // TODO 4.把 文件系统 抽象成 Table： Connect
        // 1.调用 connect，连接外部系统
        // 2.调用 withFormat，指定数据格式
        // 3.调用 withSchema，指定 抽象成的 Table的 表结构（字段名、字段类型）
        // 4.调用 createTemporaryTable，指定 Table的表名
        tableEnv
                .connect(new FileSystem().path("output/flink.txt"))
                .withFormat(new OldCsv().fieldDelimiter("|"))
                .withSchema(
                        new Schema()
                                .field("a", DataTypes.STRING())
                                .field("tt", DataTypes.BIGINT())
                                .field("cc", DataTypes.INT())
                )
                .createTemporaryTable("fsTable");

        // TODO 5.通过 往 表插入数据的方法，把结果 Sink到文件系统
        TableResult fsTable = resultTable.executeInsert("fsTable");
        fsTable.print();


        env.execute();
    }
}
