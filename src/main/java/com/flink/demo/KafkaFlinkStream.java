package com.flink.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.time.Duration;
import java.util.Properties;

public class KafkaFlinkStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<User> source = KafkaSource.<User>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("users_topic")
                .setGroupId("my-group2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new UserDeserializer())
                .build();

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "60000");

        KafkaSink<User> sink = KafkaSink.<User>builder()
                .setBootstrapServers("localhost:9092")
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(new MyRecordSerializer("user_filtered_topic"))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("my-trx-id-prefix")
                //.setRecordSerializer(KafkaRecordSerializationSchema.builder()
                   //     .setTopic("user_filtered_topic")
                     //   .setValueSerializationSchema(new UserSerializer())
                       // .build()
                //)
                .build();



        DataStream<User> userStream =
                env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Users");


        userStream.filter(s -> "Region_5".equals(s.getRegionid())).sinkTo(sink);


        env.execute("test");






    }
}
