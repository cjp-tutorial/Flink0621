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


        //TODO
        // kafka 的offset 保存在 Source算子的状态
        // kafka不知道offset更新到哪，用一些监控kafka的工具，看不到offset的进展
        // setCommitOffsetsOnCheckpoints设为 true， 会同步一份给kafka，就可以看到了
        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "sensor0621",
                        new SimpleStringSchema(),
                        properties)
                        .setStartFromEarliest()
                        .setCommitOffsetsOnCheckpoints(true)

        );

        //TODO sink到文件，怎么使用 2pc
        // 开启事务 ：创建一个临时文件
        // 预提交 ： 往 临时文件 写入数据
        // 正式提交： 把临时文件，改名成正式名字
        // 回滚：     把临时文件删除

        kafkaSource.print();


        env.execute();
    }
}
