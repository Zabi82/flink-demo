package com.flink.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.sql.Timestamp;
import java.time.Duration;

public class UnionOfDifferentStreamSources {

    public static final String INPUT_DIRECTORY = "input";

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<User> source = KafkaSource.<User>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("users_topic")
                .setGroupId("my-group2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new UserDeserializer())
                .build();


        String directory = parameters.has(INPUT_DIRECTORY)  ? parameters.get(INPUT_DIRECTORY) : "/home/zabeer/flink_input";

        DataStream<User> userStreamFromKafka =
                env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Users");

        final FileSource<String> fileSource =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), Path.fromLocalFile(new File(directory)))
                        .monitorContinuously(Duration.ofSeconds(1L))
                        .build();
        final DataStream<User> userStreamFromFile =
                env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source").map(new UserMapper());

        userStreamFromKafka.union(userStreamFromFile).print();


        env.execute("test");






    }

    public static final class UserMapper implements MapFunction<String, User> {


        @Override
        public User map(String s) throws Exception {
            if(!StringUtils.isNullOrWhitespaceOnly(s)) {
                String[] tokens = s.split(",");
                if(tokens.length == 3) {
                    return new User(tokens[0], tokens[1], tokens[2], new Timestamp(System.currentTimeMillis()));
                }
            }
            return new User();
        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}
