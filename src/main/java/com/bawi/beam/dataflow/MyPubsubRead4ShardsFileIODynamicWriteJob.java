package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
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

import static com.bawi.beam.dataflow.LogUtils.*;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.beam.dataflow.PubSubUtils.createTopicAndSubscription;
import static com.bawi.beam.dataflow.PubSubUtils.deleteSubscriptionAndTopic;
import static java.lang.Thread.currentThread;

public class MyPubsubRead4ShardsFileIODynamicWriteJob {
    public static void main(String[] args) throws IOException {
        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
        createTopicAndSubscription(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);

        String[] writeArgs = updateArgsWithDataflowRunner(args, "--topic=" + TOPIC, "--jobName=" + OWNER + "-pubsub-write");
        MyPipelineOptions writeOptions = PipelineOptionsFactory.fromArgs(writeArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline writingPipeline = Pipeline.create(writeOptions);

        writingPipeline.apply(Create.of(IntStream.rangeClosed(1, 10000).mapToObj(String::valueOf).collect(Collectors.toList())))
                .apply(ParDo.of(new CreatePubsubMessageFn()))
                .apply(PubsubIO.writeMessages().to(writeOptions.getTopic()));

        writingPipeline.run().waitUntilFinish();


        String[] readArgs = updateArgsWithDataflowRunner(args,
                "--jobName=" + OWNER + "-" + MyPubsubRead4ShardsFileIODynamicWriteJob.class.getSimpleName().toLowerCase(),
                "--subscription=" + SUBSCRIPTION,
                "--numWorkers=2",
                "--maxNumWorkers=2",
                "--numberOfWorkerHarnessThreads=4",
                "--workerMachineType=n1-standard-2",
                "--output=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/" + MyPubsubRead4ShardsFileIODynamicWriteJob.class.getSimpleName() + "/output",
                "--temp=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/" + MyPubsubRead4ShardsFileIODynamicWriteJob.class.getSimpleName() + "/temp"
        );
        MyPipelineOptions readOptions = PipelineOptionsFactory.fromArgs(readArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(readOptions);

        readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(readOptions.getSubscription()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply("Log Window", ParDo.of(new DoFn<PubsubMessage,  KV<String, String>>() {
                    @ProcessElement
                    public void process(@Element PubsubMessage e, OutputReceiver<KV<String, String>> receiver, @Timestamp Instant ts, BoundedWindow w, PaneInfo p) {
                        int i = Integer.parseInt(new String(e.getPayload()));
                        String key = i < 10 ? "a" : i < 100 ? "b" : i < 1000 ? "c" : "d";
                        KV<String, String> kv = KV.of(key, String.valueOf(i));
                        LOGGER.info("[{}][Window] {}:{},ts={},w={},p={}", ipAddressAndThread(), key, i, ts, windowToString(w), p);
                        receiver.output(kv);
                    }
                }))
                .apply(FileIO
                    .<String, KV<String, String>>writeDynamic()
                    .by(KV::getKey)
                    .via(Contextful.fn(KV::getValue), TextIO.sink())
                    .withDestinationCoder(StringUtf8Coder.of())
                    .withNaming(subPath -> new MyFileNaming(subPath, ".txt"))
                    .to(readOptions.getOutput())
                    .withTempDirectory(readOptions.getTemp())
                    .withNumShards(4)
                );

        readingPipeline.run().waitUntilFinish();

        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubRead4ShardsFileIODynamicWriteJob.class);

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

        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getTemp();
        void setTemp(ValueProvider<String> value);
    }

    static class CreatePubsubMessageFn extends DoFn<String, PubsubMessage> {
        private static final Map<String, String> MAP = Stream.of(new String[][]{{"key1", "value1" }, {"key2", "value2" }})
                .collect(Collectors.toMap(kvArr -> kvArr[0], kvArr -> kvArr[1]));

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<PubsubMessage> outputReceiver) {
            outputReceiver.output(new PubsubMessage(word.getBytes(), MAP));
        }
    }

    static class MyFileNaming implements FileIO.Write.FileNaming {
        private final String subPath;
        private final String extension;

        public MyFileNaming(String subPath, String extension) {
            this.subPath = subPath;
            this.extension = extension;
        }

        @Override
        public String getFilename(BoundedWindow w, PaneInfo p, int numShards, int shardIndex, Compression compression) {
            String filename = String.format("%s-winMaxTs-%s-shard-%s-of-%s--%s-%s%s", subPath, w.maxTimestamp().toString().replace(":","_").replace(" ","_"), shardIndex, numShards, getLocalHostAddressSpaced(), currentThread().getName(), extension);
            LOGGER.info("[{}][Write] Writing data to subPath='{}',w={},p={}", ipAddressAndThread(), filename, windowToString(w), p);
            return filename;
        }
    }
}

/*
.withNumShards(4)
Generated 16 shards – 4 shards for each key

a-winMaxTs-2023-01-20T10_48_04.999Z-shard-0-of-4--10_128_0_112-Thread-17.txt
a-winMaxTs-2023-01-20T10_48_04.999Z-shard-1-of-4--10_128_0_112-Thread-16.txt
a-winMaxTs-2023-01-20T10_48_04.999Z-shard-2-of-4--10_128_0_112-Thread-16.txt
a-winMaxTs-2023-01-20T10_48_04.999Z-shard-3-of-4--10_128_0_112-Thread-16.txt
b-winMaxTs-2023-01-20T10_48_04.999Z-shard-0-of-4--10_128_0_112-Thread-17.txt
b-winMaxTs-2023-01-20T10_48_04.999Z-shard-1-of-4--10_128_0_112-Thread-16.txt
b-winMaxTs-2023-01-20T10_48_04.999Z-shard-2-of-4--10_128_0_112-Thread-16.txt
b-winMaxTs-2023-01-20T10_48_04.999Z-shard-3-of-4--10_128_0_112-Thread-17.txt
c-winMaxTs-2023-01-20T10_48_04.999Z-shard-0-of-4--10_128_0_112-Thread-16.txt
c-winMaxTs-2023-01-20T10_48_04.999Z-shard-1-of-4--10_128_0_112-Thread-17.txt
c-winMaxTs-2023-01-20T10_48_04.999Z-shard-2-of-4--10_128_0_112-Thread-16.txt
c-winMaxTs-2023-01-20T10_48_04.999Z-shard-3-of-4--10_128_0_112-Thread-17.txt
d-winMaxTs-2023-01-20T10_48_04.999Z-shard-0-of-4--10_128_0_112-Thread-16.txt
d-winMaxTs-2023-01-20T10_48_04.999Z-shard-1-of-4--10_128_0_112-Thread-18.txt
d-winMaxTs-2023-01-20T10_48_04.999Z-shard-2-of-4--10_128_0_112-Thread-17.txt
d-winMaxTs-2023-01-20T10_48_04.999Z-shard-3-of-4--10_128_0_112-Thread-16.txt
However, when

.withNumShards(4)
is changed to:

.withNumShards(0)
then the runner will determine number of shards per key (assuming Streaming Engine is used), in that case we get 20 shards (additional 4 shards for hottest key d):
a-winMaxTs-2023-01-20T10_27_34.999Z-shard-0-of-4–10_128_0_7-Thread-17.txt
a-winMaxTs-2023-01-20T10_27_34.999Z-shard-1-of-4–10_128_0_7-Thread-16.txt
a-winMaxTs-2023-01-20T10_27_34.999Z-shard-2-of-4–10_128_0_7-Thread-16.txt
a-winMaxTs-2023-01-20T10_27_34.999Z-shard-3-of-4–10_128_0_7-Thread-17.txt
b-winMaxTs-2023-01-20T10_27_34.999Z-shard-0-of-4–10_128_0_7-Thread-16.txt
b-winMaxTs-2023-01-20T10_27_34.999Z-shard-1-of-4–10_128_0_7-Thread-17.txt
b-winMaxTs-2023-01-20T10_27_34.999Z-shard-2-of-4–10_128_0_7-Thread-17.txt
b-winMaxTs-2023-01-20T10_27_34.999Z-shard-3-of-4–10_128_0_7-Thread-16.txt
c-winMaxTs-2023-01-20T10_27_34.999Z-shard-0-of-4–10_128_0_7-Thread-17.txt
c-winMaxTs-2023-01-20T10_27_34.999Z-shard-1-of-4–10_128_0_7-Thread-17.txt
c-winMaxTs-2023-01-20T10_27_34.999Z-shard-2-of-4–10_128_0_7-Thread-17.txt
c-winMaxTs-2023-01-20T10_27_34.999Z-shard-3-of-4–10_128_0_7-Thread-16.txt
d-winMaxTs-2023-01-20T10_27_34.999Z-shard-0-of-4–10_128_0_7-Thread-17.txt
d-winMaxTs-2023-01-20T10_27_34.999Z-shard-1-of-4–10_128_0_7-Thread-16.txt
d-winMaxTs-2023-01-20T10_27_34.999Z-shard-2-of-4–10_128_0_7-Thread-17.txt
d-winMaxTs-2023-01-20T10_27_34.999Z-shard-3-of-4–10_128_0_7-Thread-16.txt
d-winMaxTs-2023-01-20T10_27_39.999Z-shard-0-of-4–10_128_0_7-Thread-19.txt
d-winMaxTs-2023-01-20T10_27_39.999Z-shard-1-of-4–10_128_0_6-Thread-16.txt
d-winMaxTs-2023-01-20T10_27_39.999Z-shard-2-of-4–10_128_0_7-Thread-19.txt
d-winMaxTs-2023-01-20T10_27_39.999Z-shard-3-of-4–10_128_0_6-Thread-16.txt
*/