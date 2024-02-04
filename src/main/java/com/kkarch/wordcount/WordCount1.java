package com.kkarch.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * @author wangkai
 * @date 2024/2/4 15:00
 **/
public class WordCount1 {

    public static void main(String[] args) throws Exception {
        // 写一个 flink 版本的 wordcount
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建Kafka消费者
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties);

        // 添加Kafka消费者到执行环境
        DataStream<String> text = env.addSource(kafkaConsumer);

        // 数据转换和聚合
        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        // 输出结果
        counts.print();

        // 执行任务
        env.execute("WordCount Example");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line into words
            String[] words = value.toLowerCase().split("\\W+");

            // emit the words
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
