package chapter07;

import bean.PunctuatedWatermarkGenerator;
import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/7 8:58
 */
public class Flink03_CEP_API {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
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
//                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(4))
//                                .withTimestampAssigner((sensor, recordTs) -> sensor.getTs() * 1000L)
                                .forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                                    @Override
                                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                        return new PunctuatedWatermarkGenerator<WaterSensor>();
                                    }
                                })
                                .withTimestampAssigner((data, ts) -> data.getTs() * 1000L)
                );

        // TODO 1.定义规则
        // times(m): 事件发生的次数，类似 宽松近邻的关系， 仅作用于当前事件
        // times(m,n):事件发生的次数，仅作用于当前事件， 包含 m次 到 n次， 比如 （1，3） =》 1次、2次、3次都算

        // within(Time):超时， 匹配必须在指定时间内完成，超时则匹配失败，达到临界时间就算超时
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .times(2);
                .next("next")
//                .followedBy("f")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .times(2);
//                .times(1, 3);
                .within(Time.seconds(2));


        // TODO 2.应用规则：将规则应用到流上
        PatternStream<WaterSensor> sensorPS = CEP.pattern(sensorDS, pattern);

        // TODO 3.处理结果
        SingleOutputStreamOperator<String> resultDS = sensorPS.select(
                new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                        // map ===> ("事件名"， 匹配上的数据)
                        return pattern.toString();
                    }
                }
        );

        resultDS.print();

        env.execute();

    }
}
