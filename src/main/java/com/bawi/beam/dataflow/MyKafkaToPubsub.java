package com.bawi.beam.dataflow;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyKafkaToPubsub {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyKafkaToPubsub.class);

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
        pipeline.apply(KafkaIO.<Long, String>read()
                    .withBootstrapServers("localhost:9092")
                    .withTopic("my-topic")
                    .withKeyDeserializer(LongDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withMaxNumRecords(2) // converts to bounded collection reading only 2 records and stopping
//                    .withMaxReadTime(Duration.standardSeconds(30)) // converts to bounded collection reading only for 30 secs and stopping
                .withReadCommitted()
                .withProcessingTime()
                .commitOffsetsInFinalize()
                .withConsumerConfigUpdates(
                        ImmutableMap.of("enable.auto.commit", true, ConsumerConfig.GROUP_ID_CONFIG, "my-group")
                )
                .commitOffsetsInFinalize()
                )
                .apply(MapElements.into(TypeDescriptors.strings()).via(kafkaRecord -> {
                    Headers headers = kafkaRecord.getHeaders();
                    KV<Long, String> kv = kafkaRecord.getKV();
                    LOGGER.info("[LOGGER] !!!! Processing headers: {} and value: {}", headers, kv.getValue());
                    System.out.println("[Console] !!!! Processing headers: " + headers + " and value: " + kv.getValue());
                    return kv.getValue();
                }));
        pipeline.run().waitUntilFinish();
    }
}

//me@MacBook:~/Downloads/kafka_2.11-1.1.1$ rm -rf /tmp/zookeeper/ /tmp/kafka*
//me@MacBook:~/Downloads/kafka_2.11-1.1.1$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
//
//me@MacBook:~/Downloads/kafka_2.11-1.1.1$ ./bin/kafka-server-start.sh config/server.properties
//
//me@MacBook:~/Downloads/kafka_2.11-1.1.1$ ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic my-topic
//Created topic "my-topic".
//me@MacBook:~/Downloads/kafka_2.11-1.1.1$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic
//>vvv
//>xxxx
//>yyyy^
