package chapter05;

import bean.AdClickLog;
import bean.OrderEvent;
import bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 实时对账
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink29_Case_OrderTxAnalysis {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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
                });

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
                });

        // 3. 处理数据： 对两条流进行关联，匹配 交易码
        // 3.1 连接两条流, 正常 使用 connect，要考虑 做 keyby
        ConnectedStreams<OrderEvent, TxEvent> orderTxCS = orderDS
                .keyBy(order -> order.getTxId())
                .connect(txDS.keyBy(tx -> tx.getTxId()));
        // 3.2 使用 Process
        SingleOutputStreamOperator<String> resultDS = orderTxCS
//                .keyBy(order -> order.getTxId(), tx -> tx.getTxId())
                .process(new OrderTxDetect());

        // 4.输出
        resultDS.print();

        env.execute();
    }

    public static class OrderTxDetect extends CoProcessFunction<OrderEvent, TxEvent, String> {

        // 缓存 交易系统 的数据
        private Map<String, TxEvent> txEventMap = new HashMap();
        // 缓存 业务系统 的数据
        private Map<String, OrderEvent> orderEventMap = new HashMap();

        /**
         * 处理 业务系统 数据
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // 说明来的是 业务数据
            // 判断 对应交易码的交易数据 是否来过？
            if (value.getTxId() != null) {
                if (txEventMap.containsKey(value.getTxId())) {
                    // 1.交易数据 来过 => 对账成功,清除 交易码对应的 交易数据 的缓存
                    out.collect("订单" + value.getOrderId() + "对账成功！！！");
                    txEventMap.remove(value.getTxId());
                } else {
                    // 2.交易数据 没来过 => 把 自己（业务数据） 缓存起来
                    orderEventMap.put(value.getTxId(), value);
                }
            }
        }

        /**
         * 处理 交易系统 数据
         *
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
            // 说明来的是 交易数据
            // 判断 对应交易码的业务数据 是否来过
            if (orderEventMap.containsKey(value.getTxId())) {
                // 1.说明 业务数据 来过 => 对账成功， 清除 业务数据的缓存
                out.collect("订单" + orderEventMap.get(value.getTxId()).getOrderId() + "对账成功！！！");
                orderEventMap.remove(value.getTxId());
            } else {
                // 2.说明 业务数据 没来过 => 把自己（交易数据）缓存起来
                txEventMap.put(value.getTxId(), value);
            }
        }
    }
}
