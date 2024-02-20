package com.atguigu;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L);
        env.setStateBackend(new HashMapStateBackend());

        KafkaSource<String> kafkaSource1 = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topic_db")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setGroupId("test01")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaSource1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaSource.print();

        env.execute();


    }
}
