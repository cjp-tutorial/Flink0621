package chapter07;

import bean.OrderEvent;
import bean.TxEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink07_Case_OrderTxAnalysisWithIntervalJoin {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.读取数据
        // 2.1 读取 业务系统 数据
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
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((order, ts) -> order.getEventTime() * 1000L)
                );

        // 2.2 读取 交易系统 数据
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new TxEvent(
                                datas[0],
                                datas[1],
                                Long.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TxEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tx, ts) -> tx.getEventTime() * 1000L)
                );

        // TODO 3. 处理数据： 使用 IntervalJoin实现： 必须基于 事件时间
        // 1.分别对两条流进行keyby，key作为连接条件
        KeyedStream<OrderEvent, String> orderKS = orderDS.keyBy(order -> order.getTxId());
        KeyedStream<TxEvent, String> txKS = txDS.keyBy(tx -> tx.getTxId());
        // 2.调用 intervaljoin，指定时间范围
        orderKS
                .intervalJoin(txKS)
                .between(Time.hours(-1), Time.hours(1))
                .process(
                        new ProcessJoinFunction<OrderEvent, TxEvent, String>() {
                            /**
                             *
                             * left.intervaljoin(right)
                             * @param left 左边流的数据
                             * @param right 右边流的数据
                             * @param ctx 上下文
                             * @param out 采集器
                             * @throws Exception
                             */
                            @Override
                            public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<String> out) throws Exception {
                                // 进到这个方法的 left和right数据，是 join上的数据，不需要再判断 连接条件是否相等
                                out.collect(left + " <----> " + right);
                            }
                        }
                )
                .print();

        //TODO interval join 源码简析
        // 1. 底层 使用 connect连接两条流，keyby作为条件
        // 2. isLate（）判断是否迟到： et < watermark,认为迟到，不处理  =》 watermark 以两条流中，小的为准
        // 3. 两条流都使用了一个 MapState用来 缓存数据， key是 事件时间， value是 List形式的 数据
        // 4. 怎么实现join？ => 遍历对方的 MapState，在时间范围内，匹配上，就进入 用户定义的 processElement 方法
        // 5. Map有其清空策略，不会 一直 缓存



        // TODO 清空的时间分析： 例，时间= 10s， 下界 = -2s， 上界 = 2s (视情况去看)
        // 1. left调用
//        public void processElement1(StreamRecord<T1> record) throws Exception {
//            processElement(record, leftBuffer, rightBuffer, lowerBound, upperBound, true);
//        }
//        =>   left + lowerBound <= right.ts <= left + upperBound

        // 2. right 调用
//        public void processElement2(StreamRecord<T2> record) throws Exception {
//            processElement(record, rightBuffer, leftBuffer, -upperBound, -lowerBound, false);
//        }
//        =>   right - upperBound <= left.ts <= right - lowerBound


        // 1. 注册 清理数据的 定时器
//        long cleanupTime = (relativeUpperBound > 0L) ? ourTimestamp + relativeUpperBound : ourTimestamp;
//        => 为什么判断大于0呢，因为如果是 right流，传参的上下界 调换了顺序、加了 负 号
//        => 如果当前数据是 left，那么 relativeUpperBound = upperBound = 2s,   cleanupTIme = 10s + 2s = 12s 清空
//        => 如果当前数据是 right，那么 relativeUpperBound = -lowerBound = 2s, cleanupTime = 10s + 2s = 12s 清空
//        if (isLeft) {
//            internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, cleanupTime);
//        } else {
//            internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, cleanupTime);
//        }

//         left清空： leftBuffer.remove(12s - 2s) =》在12s的时候清空 et=10s的 left数据
//        case CLEANUP_NAMESPACE_LEFT: {
//            long timestamp = (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;
//            logger.trace("Removing from left buffer @ {}", timestamp);
//            leftBuffer.remove(timestamp);
//            break;
//        }

//        right清空：rightBuffer.remove(10s) =》在 12s 清空 et=10s的数据
//        case CLEANUP_NAMESPACE_RIGHT: {
//            long timestamp = (lowerBound <= 0L) ? timerTimestamp + lowerBound : timerTimestamp;
//            logger.trace("Removing from right buffer @ {}", timestamp);
//            rightBuffer.remove(timestamp);
//            break;
//        }


        env.execute();
    }

}
