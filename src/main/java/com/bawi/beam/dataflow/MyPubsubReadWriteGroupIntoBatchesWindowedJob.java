package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
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

import static com.bawi.beam.dataflow.LogUtils.ipAddressAndThread;
import static com.bawi.beam.dataflow.LogUtils.windowToString;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.beam.dataflow.PubSubUtils.createTopicAndSubscription;
import static com.bawi.beam.dataflow.PubSubUtils.deleteSubscriptionAndTopic;

public class MyPubsubReadWriteGroupIntoBatchesWindowedJob {
    public static void main(String[] args) throws IOException {
        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
        createTopicAndSubscription(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);

        String[] writeArgs = updateArgsWithDataflowRunner(args, "--topic=" + TOPIC, "--jobName=" + OWNER + "-pubsub-read");
        MyPipelineOptions writeOptions = PipelineOptionsFactory.fromArgs(writeArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline writingPipeline = Pipeline.create(writeOptions);

        writingPipeline.apply(Create.of(IntStream.rangeClosed(1, 10000).mapToObj(String::valueOf).collect(Collectors.toList())))
                .apply(ParDo.of(new CreatePubsubMessageFn()))
                .apply(PubsubIO.writeMessages().to(writeOptions.getTopic()));

        writingPipeline.run().waitUntilFinish();


        String[] readArgs = updateArgsWithDataflowRunner(args,
                "--jobName=" + OWNER + "-" + MyPubsubReadWriteGroupIntoBatchesWindowedJob.class.getSimpleName().toLowerCase(),
                "--subscription=" + SUBSCRIPTION,
                "--numWorkers=2",
                "--maxNumWorkers=2",
                "--numberOfWorkerHarnessThreads=4",
                "--workerMachineType=n1-standard-2"
        );
        MyPipelineOptions readOptions = PipelineOptionsFactory.fromArgs(readArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(readOptions);

        readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(readOptions.getSubscription()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(60))))
                .apply("Log processing", ParDo.of(new DoFn<PubsubMessage, KV<Integer, Integer>>() {
                    @ProcessElement
                    public void process(@Element PubsubMessage element, @Timestamp Instant ts, BoundedWindow w, PaneInfo p, OutputReceiver<KV<Integer, Integer>> outputReceiver) {
                        int i = Integer.parseInt(new String(element.getPayload()));
                        KV<Integer, Integer> kv = KV.of(i % 100, i);
                        LOGGER.info("[{}][Window] {}:{},ts={},w={},p={}", ipAddressAndThread(), i, i % 100, ts, windowToString(w), p);
                        outputReceiver.output(kv);
                    }
                }))
                .apply(GroupIntoBatches.ofSize(5))
                .apply("Log batches", ParDo.of(new DoFn<KV<Integer, Iterable<Integer>>, Void>() {
                    @ProcessElement
                    public void process(@Element KV<Integer, Iterable<Integer>> element, @Timestamp Instant ts, BoundedWindow w, PaneInfo p) {
                        LOGGER.info("[{}][Batched] {}:{},ts={},w={},p={}", ipAddressAndThread(), element.getKey(), element.getValue(), ts, windowToString(w), p);
                    }
                }));

        readingPipeline.run().waitUntilFinish();

        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }


    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubReadWriteGroupIntoBatchesWindowedJob.class);

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

//2023-01-21 08:11:37.144 UTC Status server started on http://10.128.0.117:8081/
//2023-01-21 08:11:38.503 UTC Status server started on http://10.128.0.47:8081/
//
//2023-01-21 08:11:38.940 UTC [10.128.0.117|i:36|n:Thread-19][Window] 1001:1,ts=2023-01-21T08:06:51.965Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
//2023-01-21 08:11:41.595 UTC [10.128.0.47 |i:36|n:Thread-18][Batched] 1:[1001, 1101, 101, 901, 2401],ts=2023-01-21T08:06:52.393Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
//
//2023-01-21 08:11:43.278 UTC [10.128.0.47 |i:34|n:Thread-16][Window] 9775:75,ts=2023-01-21T08:06:53.985Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
//2023-01-21 08:11:43.587 UTC [10.128.0.117|i:33|n:Thread-16][Batched] 75:[6975, 7875, 8675, 7775, 9775],ts=2023-01-21T08:06:53.985Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
//
//2023-01-21 08:11:42.789 UTC [10.128.0.47|i:36|n:Thread-18][Window] 3269:69,ts=2023-01-21T08:06:52.599Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
//2023-01-21 08:11:43.319 UTC [10.128.0.47|i:36|n:Thread-18][Batched] 69:[5769, 3269, 3469, 3569, 5969],ts=2023-01-21T08:06:53.192Z,w=IntervalWindow: [2023-01-21T08:06:00.000Z..2023-01-21T08:07:00.000Z),p=PaneInfo.NO_FIRING
