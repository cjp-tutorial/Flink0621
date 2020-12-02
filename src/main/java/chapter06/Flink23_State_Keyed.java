package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/12/1 8:54
 */
public class Flink23_State_Keyed {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<WaterSensor> socketDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((sensor, recordTs) -> sensor.getTs() * 1000L)
                );

        // TODO
        SingleOutputStreamOperator<WaterSensor> resultDS = socketDS
                .keyBy(r -> r.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            // TODO 1.定义状态
                            // 不能在 这里 使用 RuntimeContext => The runtime context has not been initialized.
//                            ValueState<Integer> valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                            ValueState<Integer> valueState ;
                            ListState<String> listState;
                            MapState<String,Long> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // TODO 2.在 open 里面 创建状态
                                // 通过 运行时上下文
                                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list-state", String.class));
                                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", String.class, Long.class));
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // TODO 3. 使用 状态
                                // 获取 状态的 值
//                                valueState.value();
                                // 更新 状态的 值
//                                valueState.update();
                                // 清空 当前key 对应的 状态
//                                valueState.clear();

                                // 获取 List状态的值
//                                listState.get();
                                // 添加 单个值
//                                listState.add();
                                // 添加 整个 List
//                                listState.addAll();
                                // 更新 整个 List
//                                listState.update();
                                // 清空 当前key 对应的 状态
//                                listState.clear();

                                // 根据 key 获取 value
//                                mapState.get();
                                // 清空 当前key 对应的 状态
//                                mapState.clear();
                            }
                        }
                );

        resultDS.print();


        env.execute();
    }
}
