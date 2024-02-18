package com.kkarch.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 将从 kafka 消费到的数据存储到 ListState 中，并打印
 *
 * @author wangkai
 * @date 2024/2/18 13:24
 **/
public class ListStateTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建Kafka消费者
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test1")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 添加Kafka消费者到执行环境
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(f -> new Tuple2<>(0, f))
                .returns(Types.TUPLE(Types.INT, Types.STRING))
                .keyBy(value -> value.f0)
                .flatMap(new MyListState())
                .print();

        env.execute();

    }

    private static class MyListState extends RichFlatMapFunction<Tuple2<Integer, String>, String> {

        private ListState<String> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", String.class));
        }

        @Override
        public void flatMap(Tuple2<Integer, String> value, Collector<String> out) throws Exception {
            listState.add(value.f1);
            Iterable<String> strings = listState.get();
            StringBuilder sb = new StringBuilder();
            for (String s : strings) {
                sb.append(s);
            }
            out.collect(sb.toString());
        }
    }
}
