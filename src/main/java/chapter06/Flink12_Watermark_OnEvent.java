package chapter06;

import bean.PeriodicWatermarkGenerator;
import bean.PunctuatedWatermarkGenerator;
import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink12_Watermark_OnEvent {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 调整 watermark 的 生成周期
        env.getConfig().setAutoWatermarkInterval(2000L);

        SingleOutputStreamOperator<WaterSensor> socketDS = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator(
                                        new WatermarkGeneratorSupplier<WaterSensor>() {
                                            @Override
                                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                                return new PunctuatedWatermarkGenerator<WaterSensor>();
                                            }
                                        }
                                )
                                .withTimestampAssigner((sensor, ts) -> sensor.getTs() * 1000L)
                );

        socketDS
                .keyBy(r -> r.getId())
                .timeWindow(Time.seconds(5))
                .sum("vc")
                .print();


        env.execute();
    }
}
