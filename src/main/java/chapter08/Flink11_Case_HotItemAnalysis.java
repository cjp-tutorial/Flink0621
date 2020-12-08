package chapter08;

import bean.ItemCountWithWindowEnd;
import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/4 16:14
 */
public class Flink11_Case_HotItemAnalysis {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((data, ts) -> data.getTimestamp() * 1000L)
                );

        // TODO 3.使用 FlinkSQL 实现
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 统计 每5分钟输出最近一小时的热门商品点击，只需要 商品ID、行为、时间戳
        Table userBehaviorTable = tableEnv.fromDataStream(userBehaviorDS, $("itemId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        // 3.1 数据的准备：过滤出pv行为、分组、开窗、求和统计、携带窗口信息
        Table aggTable = userBehaviorTable
                .where($("behavior").isEqual("pv"))
                .window(
                        Slide
                                .over(lit(1).hours()).every(lit(5).minutes())
                                .on($("ts"))
                                .as("w")
                )
                .groupBy($("w"), $("itemId"))
                .select($("itemId"),
                        $("itemId").count().as("itemCount"),
                        $("w").end().as("windowEnd"));

        // 把 Table转成 DataStream，再转成 Table
        DataStream<Row> aggDS = tableEnv.toAppendStream(aggTable, Row.class);
        tableEnv.createTemporaryView("aggTable", aggDS,$("itemId"),$("itemCount"),$("windowEnd"));

        // 3.2 实现 TopN
        Table tableResult = tableEnv.sqlQuery("select * " +
                "from (" +
                "select *,row_number() over(partition by windowEnd order by itemCount desc) as rn from aggTable) " +
                "where rn <= 3");

        tableEnv.toRetractStream(tableResult, Row.class).print();

        env.execute();
    }

}
