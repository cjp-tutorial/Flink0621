package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
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
public class Flink07_HiveIntegration {
    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

/*        DataStreamSource<Tuple3<String, String, Integer>> inputDS = env.fromElements(
                Tuple3.of("1", "ergou", 18),
                Tuple3.of("2", "gousheng", 19),
                Tuple3.of("3", "goudan", 20)
        );*/

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> inputDS = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return Tuple3.of(datas[0], datas[1], Integer.valueOf(datas[2]));
                    }
                });


        // TODO 2.FlinkSQL 集成 Hive

        String catalogName = "myhive"; //catalog名称
        String defaultDatabase = "flinktest"; // hive数据库，设置为这个catalog下的默认库
        String hiveConfDir = "F:\\atguigu\\01_course\\code\\hive-conf"; // hive配置文件所在的目录
//        String hiveConfDir = "/opt/module/hive/conf"; // hive配置文件所在的目录
        String version = "1.2.1";
        // TODO 1、创建 hive 的catalog
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir, version);
        // TODO 2、注册 Catalog
        tableEnv.registerCatalog(catalogName, hiveCatalog);

        // TODO 3、指定 catalog（不指定，就是有一个默认的catalog，叫 default_catalog）
        tableEnv.useCatalog(catalogName);
//        tableEnv.useDatabase(); // 指定 hive的数据库，不指定就是前面指定的 默认库
        // TODO 4、指定 sql语法为 Hive sql
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // TODO 5、使用 sql操作hive表
        tableEnv.createTemporaryView("inputTable", inputDS);
        tableEnv.executeSql("insert into test select * from inputTable");


        Table queryTable = tableEnv.sqlQuery("select * from test");
        tableEnv.toAppendStream(queryTable, Row.class).print();

        env.execute();
    }
}
