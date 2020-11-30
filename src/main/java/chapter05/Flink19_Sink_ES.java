package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/28 11:13
 */
public class Flink19_Sink_ES {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost",9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        // TODO Sink ElasticSearch
        // Builder的第一个参数
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));

        // Builder第二个参数
        MyElasticSearchSinkSFunction myElasticSearchSinkSFunction = new MyElasticSearchSinkSFunction();

        ElasticsearchSink.Builder<WaterSensor> esBuilder = new ElasticsearchSink.Builder<>(httpHosts, myElasticSearchSinkSFunction);

        // 设置bulk的容量，1条就刷写
        // TODO 生产环境不要设置为 1，影响性能，这里只是为了 快速的看到 无界流 写入 ES 的结果
        esBuilder.setBulkFlushMaxActions(1);

        sensorDS.addSink(esBuilder.build());

        env.execute();
    }

    public static class MyElasticSearchSinkSFunction implements ElasticsearchSinkFunction<WaterSensor> {

        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
            Map<String, String> sourceMap = new HashMap<String, String>();
            sourceMap.put("data", element.toString());
            // 创建一个Request
            IndexRequest indexRequest = Requests.indexRequest("sensor0621").type("read").source(sourceMap);
            // 放入 indexer
            indexer.add(indexRequest);
        }
    }


}
