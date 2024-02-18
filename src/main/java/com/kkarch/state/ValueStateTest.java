package com.kkarch.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用 valueState 对 数字进行累加
 *
 * @author wangkai
 * @date 2024/2/18 10:14
 **/
public class ValueStateTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建Kafka消费者
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test1")
                .setGroupId("test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        // 添加Kafka消费者到执行环境
        DataStream<String> text = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 数据转换和聚合
        text.map(f -> new Tuple2<>(0, f))
                .returns(Types.TUPLE(Types.INT, Types.STRING))
                .keyBy((KeySelector<Tuple2<Integer, String>, Integer>) value -> value.f0)
                .flatMap(new MySum())
                .print();

        // 执行任务
        env.execute("WordCount Example");
    }

    private static class MySum extends RichFlatMapFunction<Tuple2<Integer, String>, Integer> {

        private ValueState<Integer> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Integer.class));
        }

        @Override
        public void flatMap(Tuple2<Integer, String> tuple2, Collector<Integer> out) throws Exception {
            String value = tuple2.f1;
            Integer currentSum = sum.value();
            if (currentSum == null) {
                currentSum = 0;
            }
            currentSum = currentSum + Integer.parseInt(value);
            sum.update(currentSum);
            out.collect(sum.value());
        }
    }
}

