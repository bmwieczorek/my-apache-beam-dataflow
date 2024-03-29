package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.beam.dataflow.PubSubUtils.createTopicAndSubscription;
import static com.bawi.beam.dataflow.PubSubUtils.deleteSubscriptionAndTopic;

public class MyPubsubReadWriteGroupByKeyJob {
    public static void main(String[] args) throws IOException {
        createTopicAndSubscription(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);

        String[] writeArgs = updateArgsWithDataflowRunner(args, "--topic=" + TOPIC, "--jobName=" + OWNER + "-pubsub-write");
        MyPipelineOptions writeOptions = PipelineOptionsFactory.fromArgs(writeArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline writingPipeline = Pipeline.create(writeOptions);

        writingPipeline.apply(Create.of(IntStream.rangeClosed(1, 10000).mapToObj(String::valueOf).collect(Collectors.toList())))
                .apply(ParDo.of(new CreatePubsubMessageFn()))
                .apply(PubsubIO.writeMessages().to(writeOptions.getTopic()));

        writingPipeline.run().waitUntilFinish();


        String[] readArgs = updateArgsWithDataflowRunner(args,
                "--jobName=" + OWNER + "-pubsub-groupbykey-read",
                "--subscription=" + SUBSCRIPTION,
                "--numWorkers=2",
                "--maxNumWorkers=2",
                "--numberOfWorkerHarnessThreads=4",
                "--workerMachineType=n1-standard-2"
        );
        MyPipelineOptions readOptions = PipelineOptionsFactory.fromArgs(readArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(readOptions);

        readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(readOptions.getSubscription()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply("Log Window", ParDo.of(new DoFn<PubsubMessage,  KV<String, Integer>>() {
                    @ProcessElement
                    public void process(@Element PubsubMessage element, OutputReceiver<KV<String, Integer>> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                        String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
                        int i = Integer.parseInt(new String(element.getPayload()));
                        String key = i < 10 ? "a" : i < 100 ? "b" : i < 1000 ? "c" : "d";
                        KV<String, Integer> kv = KV.of(key, i);
                        LOGGER.info("[{}][Window] Processing {}:{},ts={},w={},p={}", LogUtils.ipAddressAndThread(), key, i, timestamp, windowString, paneInfo);
                        receiver.output(kv);
                    }
                }))
                .apply(GroupByKey.create())
                .apply("Log GBK", ParDo.of(new DoFn<KV<String, Iterable<Integer>>, Void>() {
                    @ProcessElement
                    public void process(@Element KV<String, Iterable<Integer>> element, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                        String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
                        LOGGER.info("[{}][GKB] Processing {}:{},ts={},w={},p={}", LogUtils.ipAddressAndThread(), element.getKey(), element.getValue(), timestamp, windowString, paneInfo);
                    }
                }));


        readingPipeline.run().waitUntilFinish();

        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubReadWriteGroupByKeyJob.class);

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String OWNER = System.getenv("GCP_OWNER");
    private static final String TOPIC_NAME = OWNER + "-" + "topic";
    private static final String TOPIC = "projects/" + PROJECT + "/topics/" + TOPIC_NAME;
    private static final String SUBSCRIPTION_NAME = OWNER + "-" + "topic" + "-sub";
    private static final String SUBSCRIPTION = "projects/" + PROJECT + "/subscriptions/" + SUBSCRIPTION_NAME;

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getTopic();

        void setTopic(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getSubscription();

        void setSubscription(ValueProvider<String> value);
    }

    static class CreatePubsubMessageFn extends DoFn<String, PubsubMessage> {
        private static final Map<String, String> MAP = Stream.of(new String[][]{{"key1", "value1" }, {"key2", "value2" }})
                .collect(Collectors.toMap(kvArr -> kvArr[0], kvArr -> kvArr[1]));

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<PubsubMessage> outputReceiver) {
            outputReceiver.output(new PubsubMessage(word.getBytes(), MAP));
        }
    }
}

//[10.128.0.63|i:35|n:Thread-18] Processing element 9926:26
//[10.128.0.65|i:36|n:Thread-19] Batched element KV{26, [9726, 9526, 9626, 9426, 9926]}
//
//[10.128.0.65|i:34|n:Thread-17] Processing element 9475:75
//[10.128.0.63|i:34|n:Thread-17] Batched element KV{75, [9475, 9575, 9675, 9775, 9975]}
//
//[10.128.0.63|i:34|n:Thread-17] Processing element 1000:0
//[10.128.0.63|i:36|n:Thread-19] Batched element KV{0, [1300, 1600, 1700, 1900, 1000]}
//
//[10.128.0.65|i:33|n:Thread-16] Processing element 1001:1
//[10.128.0.65|i:33|n:Thread-16] Batched element KV{1, [1001, 1201, 1101, 1901, 501]}