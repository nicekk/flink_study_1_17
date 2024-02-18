package com.kkarch.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * 使用 MapState 实现对每个单词的计数统计
 *
 * @author wangkai
 * @date 2024/2/18 13:32
 **/
public class MapStateTest {

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
                .keyBy(value -> value)
                .flatMap(new MyMapFunction())
                        .print();


        env.execute();
    }

    private static class MyMapFunction extends RichFlatMapFunction<String, Tuple2<String,Integer>> {

        private MapState<String, Integer> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", String.class, Integer.class));
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // normalize and split the line into words
            String[] words = value.split("\\W+");

            // emit the words
            for (String word : words) {
                if (!word.isEmpty()) {
                    if (mapState.contains(word)) {
                        mapState.put(word, mapState.get(word) + 1);
                    } else {
                        mapState.put(word, 1);
                    }
                }
            }

            Iterable<Map.Entry<String, Integer>> entries = mapState.entries();
            for (Map.Entry<String, Integer> entry : entries) {
                out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
            }
        }
    }
}
