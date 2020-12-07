package chapter07;

import bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink05_Case_OrderTimeOutDetectWithCEP {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((data, ts) -> data.getEventTime() * 1000L)

                );

        // 3.处理数据
        // 3.1 按照 统计维度 （订单） 分组
        KeyedStream<OrderEvent, Long> orderKS = orderDS.keyBy(order -> order.getOrderId());
        // TODO 3.2 使用 CEP 实现
        // 1.定义规则
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 2.应用规则
        PatternStream<OrderEvent> orderPS = CEP.pattern(orderKS, pattern);

        // 3.获取结果
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };
        SingleOutputStreamOperator<String> resultDS = orderPS.select(
                timeoutTag,
                new OrderTimeoutFunction(),
                new PatternResultFunction()
        );


        resultDS.getSideOutput(timeoutTag).print("timeout");

        resultDS.print("result");

        env.execute();
    }

    /**
     * 定义超时数据的处理
     */
    public static class OrderTimeoutFunction implements PatternTimeoutFunction<OrderEvent, String> {

        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {

            return pattern.toString() + " <---> " + timeoutTimestamp;
        }
    }

    /**
     * 匹配上的数据的处理
     */
    public static class PatternResultFunction implements PatternSelectFunction<OrderEvent, String> {

        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            return pattern.toString();
        }
    }
}
