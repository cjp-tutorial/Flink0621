package chapter06;

import bean.OrderEvent;
import bean.TxEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import sun.awt.SunHints;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink36_Case_OrderTimeOutDetect {
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
        // 3.2 使用 process 实现
        SingleOutputStreamOperator<String> detectDS = orderKS.process(new OrderTimeOutDetectFunction());


        detectDS.print();


        env.execute();
    }

    public static class OrderTimeOutDetectFunction extends KeyedProcessFunction<Long, OrderEvent, String> {
        // 存储 支付数据
        ValueState<OrderEvent> payEvent;
        // 存储 下单数据
        ValueState<OrderEvent> createEvent;

        ValueState<Long> timeoutTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            payEvent = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payEvent", OrderEvent.class));
            createEvent = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createEvent", OrderEvent.class));
            timeoutTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeoutTs", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // TODO 1.只有一个数据来的情况
            if (timeoutTs.value() == null) {
                // 不管是create还是pay先来，都注册一个定时器，另一个来，不用再注册
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15 * 60 * 1000L);
                timeoutTs.update(ctx.timestamp() + 15 * 60 * 1000L);
            } else {
                // 说明，另一条数据 15min内来了，删除定时器
                ctx.timerService().deleteEventTimeTimer(timeoutTs.value());
                timeoutTs.clear();
            }


            //TODO 2.两个数据都会来的情况
            if ("create".equals(value.getEventType())) {
                // 1.来的是 下单的 数据 => 判断 支付数据 来过没
                OrderEvent pay = this.payEvent.value();
                if (pay == null) {
                    // 1.1 说明 支付数据 没来过 => 把自己（下单） 存起来
                    createEvent.update(value);
                } else {
                    // 1.2 说明 支付数据 来过 => 可以判断一下 下单跟支付行为之间有没有超时，看一下系统有没问题（可以不做）
                    if (pay.getEventTime() - value.getEventTime() >= 15 * 60) {
                        out.collect("订单" + value.getOrderId() + "在超时时间外完成了支付，系统可能存在漏洞！！！！");
                    } else {
                        out.collect("订单" + value.getOrderId() + "正常支付！");
                    }
                    // 无论是否超时，都已经得到了结果，把保存的 pay清空
                    payEvent.clear();
                }

            } else {
                // 2. 来的是 支付的 数据 => 判断一下 下单数据 是否来过？
                OrderEvent create = createEvent.value();
                if (create == null) {
                    // 2.1 说明 create 没来过 => 把自己 （pay） 存起来
                    payEvent.update(value);
                } else {
                    // 2.2 说明 create 来过
                    if (value.getEventTime() - create.getEventTime() >= 15 * 60) {
                        out.collect("订单" + value.getOrderId() + "在超时时间外完成了支付，系统可能存在漏洞！！！！");
                    } else {
                        out.collect("订单" + value.getOrderId() + "正常支付！");
                    }
                    createEvent.clear();
                }
            }
        }

        /**
         * 定时器触发，过了15min，另一条数据没来
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (payEvent.value() == null) {
                // 说明 pay没来 => 支付超时
                out.collect("订单" + ctx.getCurrentKey() + "支付超时，订单取消...");
//                createEvent.clear();
            }

            if (createEvent.value() == null) {
                // 说明 create没来 => 数据丢了，采集系统有问题，告警汇报
                out.collect("订单" + ctx.getCurrentKey() + "成功支付，但是没采集到下单数据，请检查系统...");
//                payEvent.clear();
            }

            createEvent.clear();
            payEvent.clear();
            timeoutTs.clear();
        }
    }
}
