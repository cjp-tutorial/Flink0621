package chapter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
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
public class Flink20_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.读数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .readTextFile("input/sensor-data.log")
//                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        // TODO Sink MySQL
        sensorDS.addSink(new MySQLSink());

        env.execute();
    }

    public static class MySQLSink extends RichSinkFunction<WaterSensor> {
        Connection conn = null;
        PreparedStatement pstmt = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建mysql连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "000000");
            pstmt = conn.prepareStatement("INSERT INTO sensor VALUES (?,?,?)");
        }

        @Override
        public void close() throws Exception {
            pstmt.close();
            conn.close();
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            pstmt.setString(1, value.getId());
            pstmt.setLong(2, value.getTs());
            pstmt.setInt(3, value.getVc());

            pstmt.execute();
        }
    }


}
