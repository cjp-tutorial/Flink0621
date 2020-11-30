package chapter05;

import bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 10:51
 */
public class Flink27_Case_APPMarketingAnalysisWithoutChannel {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读取数据
        DataStreamSource<MarketingUserBehavior> appDS = env.addSource(new AppMarketingLog());

        // 3.处理数据
        // 3.1 按照 统计维度（行为） 分组 => 求和
        appDS
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getBehavior(), 1);
                    }
                })
                .keyBy(r -> r.f0)
                .sum(1)
                .print();


        env.execute();
    }

    public static class AppMarketingLog implements SourceFunction<MarketingUserBehavior> {

        private volatile boolean isRunning = true;
        private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO", "APPSTORE");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                ctx.collect(
                        new MarketingUserBehavior(
                                random.nextLong(),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis())
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
