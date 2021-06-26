package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MyKerberizedKafkaToPubsub {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyKerberizedKafkaToPubsub.class);

    public interface Options extends PipelineOptions {
        @Validation.Required
        String getLocalKrb5Path();
        void setLocalKrb5Path(String value);

        @Validation.Required
        String getLocalKeyTabPath();
        void setLocalKeyTabPath(String value);

        @Validation.Required
        String getLocalTrustStorePath();
        void setLocalTrustStorePath(String value);

        String getGcsKrb5Path();
        void setGcsKrb5Path(String value);

        String getGcsKeyTabPath();
        void setGcsKeyTabPath(String value);

        String getGcsTrustStorePath();
        void setGcsTrustStorePath(String value);

        @Validation.Required
        String getBootstrapServers();
        void setBootstrapServers(String value);

        @Validation.Required
        String getTopic();
        void setTopic(String value);

        @Validation.Required
        String getConsumerGroupId();
        void setConsumerGroupId(String value);

        @Validation.Required
        String getPrincipal();
        void setPrincipal(String value);
    }

    public static void main(String[] args) {
        Options kerberizedOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (PipelineOptions.DirectRunner.class.getSimpleName().equals(kerberizedOptions.getRunner().getSimpleName())) {
            // use local kerberos configs
            System.setProperty("java.security.krb5.conf", kerberizedOptions.getLocalKrb5Path());
        }
        Pipeline pipeline = Pipeline.create(kerberizedOptions);

        HashMap<String, Object> props = new HashMap<>();
//        props.put("enable.auto.commit", true);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kerberizedOptions.getConsumerGroupId());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
        String jaasConfigValue = "com.sun.security.auth.module.Krb5LoginModule required " +
                "useKeyTab=true " +
                "debug=true " +
                "storeKey=true " +
                "renewTicket=true " +
                "keyTab=\"" + kerberizedOptions.getLocalKeyTabPath() + "\" " +
                "principal=\"" + kerberizedOptions.getPrincipal() + "\";";
        props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfigValue);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kerberizedOptions.getLocalTrustStorePath());

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (int partition = 0; partition < 2; partition++) {
            topicPartitions.add(new TopicPartition(kerberizedOptions.getTopic(), partition));
        }

        pipeline.apply(KafkaIO.<String, byte[]>read()
                    .withBootstrapServers(kerberizedOptions.getBootstrapServers())
                    .withTopicPartitions(topicPartitions)
                    .withKeyDeserializerAndCoder(StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                    .withValueDeserializerAndCoder(ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withReadCommitted()
                .withProcessingTime()
                .commitOffsetsInFinalize()
                .withConsumerConfigUpdates(props)
                .commitOffsetsInFinalize()
                )
                .apply(MapElements.into(TypeDescriptors.strings()).via(kafkaRecord -> {
                    Headers headers = kafkaRecord.getHeaders();
                    KV<String, byte[]> kv = kafkaRecord.getKV();
                    String value = kv.getValue() == null ? null : new String(kv.getValue());
                    LOGGER.info("[LOGGER] !!!! Processing headers: {} and value: {}", headers, value);
                    System.out.println("[Console] !!!! Processing headers: " + headers + " and value: " + value);
                    return value;
                }));
        pipeline.run().waitUntilFinish();
    }
}
