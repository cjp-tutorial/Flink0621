package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.Random;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 10:18
 */
public class Flink04_Source_UDF {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO Source - 自定义
        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySourceFunction());

        inputDS.print();

        env.execute();
    }

    public static class MySourceFunction implements SourceFunction<WaterSensor> {
        // 可见性问题
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                ctx.collect(
                        new WaterSensor("sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40)
                );
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
