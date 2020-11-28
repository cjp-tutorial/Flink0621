package chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 10:18
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO Source - 读取 kafka 的数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "aaaaaa");


        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "sensor0621",
                        new SimpleStringSchema(),
                        properties).setStartFromEarliest()
        );

        kafkaSource.print();


        env.execute();
    }
}
