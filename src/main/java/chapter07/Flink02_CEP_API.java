package chapter07;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class Flink02_CEP_API {
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
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                                .withTimestampAssigner((sensor, recordTs) -> sensor.getTs() * 1000L)
                );

        // TODO 1.定义规则
        // 模式序列：
        // 1. next():严格近邻， 要求紧挨着，中间不能有小三插足
        //    notNext(): 严格不近邻， 要求不紧挨着，中间一定要有小三插足

        // 2. followedBy:宽松近邻，只要后面出现就可以，不需要紧跟着， 找到一个就不再匹配，就像 一夫一妻
        //    notFollowedBy:宽松不近邻，后面不出现，不能作为最后一个事件，因为无界流的话，不知道会不会出现

        // 3. followedByAny():非确定性宽松近邻：后面出现就行，不限次数，来一个匹配一个， 就像 古代 一夫多妻，看上一个就纳一个妾
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
//                .next("next")
//                .notNext("next")
//                .followedBy("followedBy")
//                .notFollowedBy("notFollowedby")
                .followedByAny("followedByAny")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });
                /*.followedBy("aaaa")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                });*/


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
