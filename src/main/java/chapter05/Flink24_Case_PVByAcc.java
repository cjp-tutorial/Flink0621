package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink24_Case_PVByAcc {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2.读数据
        SingleOutputStreamOperator<UserBehavior> userbehaviorDS = env
//                .readTextFile("input/UserBehavior.csv")
                .socketTextStream("localhost", 9999)
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
                });

        // 3.处理数据
        // 3.1 能过滤就先过滤
        SingleOutputStreamOperator<UserBehavior> pvDS = userbehaviorDS.filter(sensor -> "pv".equals(sensor.getBehavior()));
        // 3.3 使用 process实现 计数
        pvDS
                .map(
                        new RichMapFunction<UserBehavior, UserBehavior>() {

                            // TODO 1.创建累加器
                            private LongCounter pvCount = new LongCounter();

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // TODO 2.注册累加器
                                getRuntimeContext().addAccumulator("pvCount", pvCount);
                            }

                            @Override
                            public UserBehavior map(UserBehavior value) throws Exception {
                                // TODO 3.使用 累加器 计数
                                pvCount.add(1L);
                                System.out.println(value + " <-------------------> " + pvCount.getLocalValue());
                                return value;
                            }
                        });

        //TODO 4.从Job的执行结果，取出累加器的值
        JobExecutionResult result = env.execute();
        Object pvCount = result.getAccumulatorResult("pvCount");
        System.out.println("统计的PV值为：" + pvCount);
    }
}
