package com.bawi.beam.dataflow;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import static java.lang.System.currentTimeMillis;
import static java.util.UUID.randomUUID;
import static org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.beam.sdk.options.ValueProvider.NestedValueProvider.of;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class MySeqGenToGCSWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySeqGenToGCSWriteJob.class);
    private static final String JOB_NAME = "bartek-" + MySeqGenToGCSWriteJob.class.getSimpleName().toLowerCase();
    private static final String PROJECT_ID = System.getenv("GCP_PROJECT");
//    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME; // single us-central1 region
    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME + "-mr"; // multiple regions (us vs us-central1)

    private static final String PAYLOAD = "462228a7-207d-46f2-88a4-092b26e6aef5,1681651204821,yULwqoD6OHhIUMMvMf6OVq2ktSUBgHLnwxn1tiLULzaaKuWEmCY73P71MR2FWGbCfNBIEIqgkcRzFNJToq83a0kYbfedkJLFCf4mQWsTdFZWlSplns94cDGCI8Z5XhyCHidnXOBcrpulgO3LfhdO9qU5VPjWiDB3LenF1mJxMRIfycyHZK7keU4ek3s2PYLhHPwuK5ZD12Ss5kmW4gJtGeY2RBVYtSfsGBe39KOiRXlZAf2r5dWhNg39rA5LGdM4z7GNG36qSDJEY4krQsHFQvhVfM3739ZcKGkm1dCeS12S2QDoyRXsww0APPlXrYUadqU5m0eHkRb47YafpzUWZi03YyY8GAjQ0Exda8XrRxvlP31I0E7pncylGijMEGwYalSgo1iVqiI9laGxiEYhjNWRzEOB1yCHoB4Rtt6zqlZY4y1Vnhcf0anSOU2kZipezHJ0jMxk4sFTbY6ZSXe4zPjy8Faz10Dm3xV3hGCPbjp2pwOKStyvjPEJIbCXLm4JI8TEKzACCKxExma6lLzbPGUrMxjJjHYFQKCrq7ojngIXqtm3vpxOEUxOQwKKuDHABqaWnG1TE4B1e7oKHl0GtM9snEoaeWwJ6IfZcLXDECqkY5gC9OMx0cfYaCuTeSnn4eCZopCeS9zb48yOBZt3UCxBeHA7D43YnPOVrtJX8kgzwXfan5RPD0ODqAmbb8pmrh3T6NzFLfFK7ejYPEd2Yzysa9q3FtlciHeLKhNhsswzumpm41qB8LDmAzaFgbm2zziEJxm3WyjbgKZI3kQJRkrrLe8DUC0RzxhH0PJnqheJM8VRoY1pLdFQAprDAdoSmp9xfuWy3vdScleKa7a8Zd1JSQf7X8X70D7t4aP2qSgxpdmRuAyYc6JDLk3iC0u9rZo46LG9VZ93twGTSfKHUuYiVQMCj50TsnpR0k4cUE9isw1pwdUqv9wYxgq9pw4ElCfTM29qSvlZtUlSaaWXzVIAkg5qwUyJNIVpmpNZ";

    public static void main(String[] args) throws IOException, InterruptedException {

        String[] updatedArgs =
            PipelineUtils.isDataflowRunnerOnClasspath() ?
                PipelineUtils.updateArgsWithDataflowRunner(args
                    , "--jobName=" + JOB_NAME + "-csv-v5-128gb-128shards-16cores-win1s-mr"
                    ,"--numWorkers=1"
                    ,"--maxNumWorkers=1"
                    ,"--workerMachineType=e2-standard-16"
                    ,"--outputDir=gs://" + BUCKET_NAME + "/"
                    ,"--tempDir=gs://" + BUCKET_NAME + "/temp/"
                    ,"--sequenceLimit=128000000"
                    ,"--numShards=128"
                ) :
                PipelineUtils.updateArgs(args
                    ,"--outputDir=target/" + JOB_NAME + "/output/"
                    ,"--tempDir=target/" + JOB_NAME + "/temp/"
                    ,"--sequenceLimit=10"
                    ,"--numShards=4"
                );

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);
//        ValueProvider<String> outputDirectory = opts.getOutputDir();


pipeline.apply(GenerateSequence.from(0).to(opts.getSequenceLimit()).withTimestampFn(i -> Instant.now()))
  .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
  .apply(ParDo.of(new GeneratePayload()))
  .apply(TextIO.write().withWindowedWrites().to(new CustomFilenamePolicy(opts.getOutputDir(), ".csv"))
    .withTempDirectory(of(opts.getTempDir(), convertToFileResourceIfPossible()))
    .withNumShards(opts.getNumShards()));

//                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.csv")));

/*
        PCollection<MyData> input = pipeline.apply(GenerateSequence.from(0).to(opts.getSequenceLimit()))
                .apply(MapElements.into(TypeDescriptor.of(MyData.class)).via(i -> {
                    MyData myData = new MyData(UUID.randomUUID().toString(), System.currentTimeMillis(), new Random().nextInt(100), RandomStringUtils.randomAlphanumeric(1000));
//                    LOGGER.info("myData={}", myData);
                    return myData;
                }));

        input.apply(AvroIO.write(MyData.class).withTempDirectory(NestedValueProvider.of(opts.getOutputTemp(), FileBasedSink::convertToFileResourceIfPossible))
                .withCodec(CodecFactory.snappyCodec())
                .to(new MyFilenamePolicy(outputDirectory, ".snappy.avro")).withNumShards(opts.getNumShards()));
//                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.snappy.avro")));


        input.apply(AvroIO.write(MyData.class).withTempDirectory(NestedValueProvider.of(opts.getOutputTemp(), FileBasedSink::convertToFileResourceIfPossible))
                .withCodec(CodecFactory.nullCodec())
//                .to(new MyFilenamePolicy(outputDirectory, ".snappy.avro")));
                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.avro")));

        input.apply(ToJson.of()).apply(TextIO.write().withTempDirectory(NestedValueProvider.of(opts.getOutputTemp(), FileBasedSink::convertToFileResourceIfPossible))
//                .to(new MyFilenamePolicy(outputDirectory, ".json")));
                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.json")));

        input.apply(ToJson.of()).apply(TextIO.write().withTempDirectory(NestedValueProvider.of(opts.getOutputTemp(), FileBasedSink::convertToFileResourceIfPossible))
                .withCompression(Compression.GZIP)
//                .to(new MyFilenamePolicy(outputDirectory, ".json.gz")));
                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.json"))); // automatically adds gz suffix


        input.apply(MapElements.into(TypeDescriptors.strings()).via(MyData::toCSV)).apply(TextIO.write().withTempDirectory(NestedValueProvider.of(opts.getOutputTemp(), FileBasedSink::convertToFileResourceIfPossible))
                .withCompression(Compression.GZIP)
//                .to(new MyFilenamePolicy(outputDirectory, ".csv.gz")));
                .withoutSharding().to(NestedValueProvider.of(outputDirectory, path -> path + "mydata.csv"))); // automatically adds gz suffix
*/

        pipeline.run().waitUntilFinish();
    }

    static class GeneratePayload extends DoFn<Long, String> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");

        @ProcessElement
        public void process(@Element Long i, OutputReceiver<String> receiver, @Timestamp Instant timestamp) {
//        public void process(@Element Long i, OutputReceiver<String> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
//            String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
            Metrics.counter(GeneratePayload.class.getSimpleName(), "window-element-counter_" + FORMATTER.print(timestamp.getMillis())).inc();
//            LOGGER.info("[{}][Window] Processing {},ts={},w={},p={}", LogUtils.ipAddressAndThread(), i, timestamp, windowString, paneInfo);
            receiver.output(i + "," + PAYLOAD);
        }
    }

    private static SerializableFunction<String, @UnknownKeyFor @NonNull @Initialized ResourceId> convertToFileResourceIfPossible() {
        return FileBasedSink::convertToFileResourceIfPossible;
    }

    static class CustomFilenamePolicy extends FileBasedSink.FilenamePolicy {
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");

        private final ValueProvider<String> outputParentPath;
        private final String filenameSuffix;

        CustomFilenamePolicy(ValueProvider<String> outputParentPath, String filenameSuffix) {
            this.outputParentPath = outputParentPath;
            this.filenameSuffix = filenameSuffix;
        }

        @Override
        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            long windowStartMillis = ((IntervalWindow) window).start().getMillis();
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            long windowStartMillis = currentTimeMillis();
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        private String getFilePath(ResourceId resource, long timestampMillis, int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            String parentDirectoryPath = resource.isDirectory() ? resource.toString() : resource.getFilename();
            String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
            String suffix = suggestedFilenameSuffix == null || suggestedFilenameSuffix.isEmpty() ? filenameSuffix : suggestedFilenameSuffix;
            String filename = String.format("%s--%s-of-%s%s", FORMATTER.print(timestampMillis), shardNumber, numShards, suffix);
//            String randomFilePrefix = DigestUtils.md5Hex(UUID.randomUUID() + filename + timestampMillis).substring(0, 6);
            String randomFilePrefix = DigestUtils.md5Hex(timestampMillis + "" + shardNumber).substring(0, 6);
            String outputFilePath = String.format("%s%s--%s", parentDirectoryPath, randomFilePrefix, filename);
//            String outputFilePath = String.format("%s%s", parentDirectoryPath, MySeqGenToGCSWriteJob.class.getSimpleName().toLowerCase() + "/" + filename);
            LOGGER.info("Writing file to {}", outputFilePath);
            return outputFilePath;
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    static class MyData implements Serializable {
        public String uuid;
        public Long ts;
        public Integer number;
        @Nullable
        public String value;

        @SuppressWarnings("unused") // used to decode
        MyData() {}

        MyData(String uuid, Long ts, Integer number, String value) {
            this.uuid = uuid;
            this.ts = ts;
            this.number = number;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyData myData = (MyData) o;
            return Objects.equals(uuid, myData.uuid) &&
                    Objects.equals(ts, myData.ts) &&
                    Objects.equals(number, myData.number) &&
                    Objects.equals(value, myData.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, ts, number, value);
        }

        @Override
        public String toString() {
            return "MyData{uuid='" + uuid + ", ts=" + ts + ", number=" + number + ", value=" + value + '}';
        }

        public String toCSV() {
            return uuid + "," + ts + "," + number + "," + value;
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutputDir();
        void setOutputDir(ValueProvider<String> value);

        ValueProvider<String> getTempDir();
        void setTempDir(ValueProvider<String> value);

        long getSequenceLimit();
        void setSequenceLimit(long value);

        int getNumShards();
        void setNumShards(int value);
    }
}
