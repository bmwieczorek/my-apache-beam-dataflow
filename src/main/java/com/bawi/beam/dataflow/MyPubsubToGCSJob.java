package com.bawi.beam.dataflow;

import com.bawi.beam.WindowUtils;
import com.bawi.beam.dataflow.schema.AvroToBigQuerySchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;

import static com.bawi.beam.dataflow.DataflowJobDetails.getMachineType;
import static com.bawi.beam.dataflow.DataflowJobSettingRecommender.getJobUpdateCurlCommand;
import static com.bawi.beam.dataflow.DataflowJobSettingRecommender.getRecommendedHighMemMachineType;
import static com.bawi.beam.dataflow.LogUtils.*;


public class MyPubsubToGCSJob {

    @SuppressWarnings("unused")
    public interface MyPipelineOptions extends PipelineOptions {

        @Validation.Required
        ValueProvider<String> getSubscription();

        void setSubscription(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutput();

        void setOutput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getTemp();

        void setTemp(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getTableSpec();

        void setTableSpec(ValueProvider<String> value);

        @Validation.Required
        boolean getMessageDeduplicationEnabled();

        void setMessageDeduplicationEnabled(boolean value);

        @Validation.Required
        boolean getCustomEventTimeTimestampAttributeEnabled();

        void setCustomEventTimeTimestampAttributeEnabled(boolean value);

        @Validation.Required
        int getWindowSecs();

        void setWindowSecs(int windowSecs);

        String getTimestampAttribute();

        void setTimestampAttribute(String timestampAttribute);

        int getNumShards();

        void setNumShards(int numShards);
    }

    /*

PROJECT=$(gcloud config get-value project)
APP=pubsub-to-console-flex
OWNER=bartek
BUCKET=$APP-$OWNER
java11

gsutil mb gs://${BUCKET}
mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=$PROJECT
gsutil cp target/classes/flex-templates/app-image-spec.json gs://${BUCKET}/

gcloud dataflow flex-template run $APP-$OWNER \
 ${GCLOUD_DATAFLOW_RUN_OPTS} \
 --template-file-gcs-location gs://${BUCKET}/app-image-spec.json  \
 --worker-machine-type n2-standard-2 --num-workers 1  --max-workers 2 \
 --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-standard,diskSizeGb=200,subscription=projects/$PROJECT/subscriptions/messages


# --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-ssd,diskSizeGb=200,workerLogLevelOverrides={\"org.apache.kafka.clients.consumer.internals.Fetcher\":\"WARN\"},subscription=projects/$PROJECT/subscriptions/messages

    */

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSJob.class);

    public static final String BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID = "bodyAttrsMsgId"; // needs to match schema column name in terraform/MyPubsubToGCSJob/bigquery.tf
    private static final Schema SCHEMA = SchemaBuilder.record("record").fields().requiredString(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID).endRecord();
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/'minute'=mm");

    public static final String MSG_ATTR_NAME = "msgAttrName";
    public static final String PUBLISH_TIME_ATTRIBUTE = "pt";
    public static final String EVENT_TIME_ATTRIBUTE = "et";

    public static void main(String[] args) {
        args = PipelineUtils.updateArgsWithDataflowRunner(args);

        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
//        options.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);
        Pipeline readingPipeline = Pipeline.create(options);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        ValueProvider<String> output = options.getOutput();
        ValueProvider<String> temp = options.getTemp();
        LOGGER.info("numShards: {}", options.getNumShards());

        PubsubIO.Read<PubsubMessage> pubsubMessageRead = PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription());

        if (options.getMessageDeduplicationEnabled()) {
            LOGGER.info("Deduplication enabled based on {}", MSG_ATTR_NAME);
            pubsubMessageRead = pubsubMessageRead.withIdAttribute(MSG_ATTR_NAME);
        }

        if (options.getCustomEventTimeTimestampAttributeEnabled()) {
            LOGGER.info("Read with custom timestamp attribute enabled based on {}", options.getTimestampAttribute());
            pubsubMessageRead = pubsubMessageRead.withTimestampAttribute(options.getTimestampAttribute());
        }

//        when static primitive backed in pipeline template
//        myMsgAttrName2idAttribute:/org.apache.beam.sdk.io.gcp.pubsub.PubsubIO$Read2
//        beam:display_data:labelled:v1
//        Timestamp Attribute et2timestampAttribute:/org.apache.beam.sdk.io.gcp.pubsub.PubsubIO$Read2
//        beam:display_data:labelled:v1

        PCollection<GenericRecord> concatBodyAndAttrsPCollection = readingPipeline
                .apply(pubsubMessageRead)
                .apply("AfterPubSub", ParDo.of(new MyBundleSizeInterceptor<>("AfterPubSub")))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSecs())))

//                            .triggering(Repeatedly.forever(
//                                    AfterFirst.of(
//                                        AfterPane.elementCountAtLeast(2),
//                                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(120))
//                                    )
//                                )
//                            )
//                            .withAllowedLateness(Duration.standardMinutes(10))
//                            .discardingFiredPanes() // if Window.into is after DoFn then DoFn logs Global Window otherwise IntervalWindow
                )
                .apply("AfterWindowInfo", ParDo.of(new MyBundleSizeInterceptor<>("AfterWindowInfo")))
                .apply(ConcatBodyAttrAndMsgIdFn.CLASS_NAME, ParDo.of(new ConcatBodyAttrAndMsgIdFn()))
                .setCoder(AvroGenericCoder.of(SCHEMA)) // required to explicitly set coder for GenericRecord
                .apply("AfterConcatBodyAttrAndMsgIdFn", ParDo.of(new MyBundleSizeInterceptor<>("AfterConcatBodyAttrAndMsgIdFn")));


        // write to BQ
        concatBodyAndAttrsPCollection.apply(BigQueryIO.<GenericRecord>write()
                .withAvroFormatFunction(r -> {
                    GenericRecord element = r.getElement();
                    LOGGER.info("[{}][{}] element {}, schema {}", getIp(), getThreadNameAndId(), element, r.getSchema());
                    return element;
                })
                .withAvroSchemaFactory(qTableSchema -> SCHEMA)
                .to(options.getTableSpec())
                .useAvroLogicalTypes()
                .withoutValidation()
                .withSchema(AvroToBigQuerySchemaConverter.convert(SCHEMA))
                .withAutoSharding() // requires streaming engine
//                .withNumFileShards(10)
                .withTriggeringFrequency(Duration.standardMinutes(1))
//                        .withLoadJobProjectId(options.getBQLoadJobProjectId())
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        // write to GCS 1
        concatBodyAndAttrsPCollection
                .apply(ParDo.of(new ToTimestampedPathKV(options.getOutput())))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroGenericCoder.of(SCHEMA)))
                .apply(ParDo.of(new MyBundleSizeInterceptor<>("ToTimestampedPathKV")))
                .apply(getStringKVWrite(output, temp, options));


        // write to GCS 2
        concatBodyAndAttrsPCollection
                .apply(AvroIO.writeGenericRecords(SCHEMA).withWindowedWrites()
                //.to(output)
                //.to(options.getOutput())
                .to(new FileBasedSink.FilenamePolicy() {

                    @SuppressWarnings("NullableProblems")
                    @Override
                    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
                        // ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(output);
                        ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(output.get());
                        IntervalWindow intervalWindow = (IntervalWindow) window;
                        String parentDirectoryPath = resource.isDirectory() ? "" : firstNonNullOrDefaultToEmpty(resource.getFilename());
                        String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
                        String suffix = suggestedFilenameSuffix != null && !suggestedFilenameSuffix.isEmpty() ? suggestedFilenameSuffix : ".avro";
                        PaneInfo.Timing pathTiming = paneInfo.getTiming();
                        String yyyyMMddHHmm = FORMATTER.print(intervalWindow.start());
                        String filename = String.format("%s/%s/%s-%s-" +
                                        "%s-%s-%s-" +
                                        "%s-%s-%s-of-%s%s",
                                parentDirectoryPath + "-windowedWrites", yyyyMMddHHmm, UUID.randomUUID(), System.currentTimeMillis(),
                                getIp(), Thread.currentThread().threadId(), Thread.currentThread().getName(), window.maxTimestamp().toString().replace(":", "_").replace(" ", "_"), pathTiming,
                                shardNumber, numShards, suffix);
                        //                                LOGGER.info("filename='{}', {}", filename, getMessage());  // output/year=2021/month=12/day=23/hour=07/minute=52/d52a2109-f8f9-459b-b055-365be2558833-1640246141797.avro
                        return resource.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
                    }

                    @SuppressWarnings("NullableProblems")
                    @Override
                    public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
                        throw new UnsupportedOperationException("Unsupported.");
                    }
                })
                // .withTempDirectory(FileBasedSink.convertToFileResourceIfPossible(output).getCurrentDirectory())
                .withTempDirectory(ValueProvider.NestedValueProvider.of(temp, t -> t != null ? FileBasedSink.convertToFileResourceIfPossible(t).getCurrentDirectory() : null))
                .withNumShards(2));

        readingPipeline.run();
    }

    private static FileIO.Write<String, KV<String, GenericRecord>> getStringKVWrite(ValueProvider<String> output, ValueProvider<String> temp, MyPipelineOptions options) {
        FileIO.Write<String, KV<String, GenericRecord>> fileIOWrite = FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                .by(kv -> kv != null ? kv.getKey() : null)
                .via(Contextful.fn(kv -> kv != null ? kv.getValue() : null), AvroIO.<GenericRecord>sink(SCHEMA).withCodec(CodecFactory.fromString("snappy")))
                .withDestinationCoder(StringUtf8Coder.of())
                .withNaming(path -> new CustomWriteFileNaming(path, "snappy", "avro"))
                .to(output)
                .withTempDirectory(temp);

        return options.getNumShards() == 0 ? fileIOWrite.withAutoSharding() : fileIOWrite.withNumShards(options.getNumShards());
    }


    static class ConcatBodyAttrAndMsgIdFn extends DoFn<PubsubMessage, GenericRecord> {
        private static final String CLASS_NAME = ConcatBodyAttrAndMsgIdFn.class.getSimpleName();
        private static final Distribution PUBSUB_MESSAGE_SIZE_BYTES = Metrics.distribution(CLASS_NAME, CLASS_NAME + "_pubsubMessageSizeBytes");
        private static final Distribution PROCESSING_TIME_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME + "_processingTimeMs");
        private static final Distribution INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME + "_inputDataFreshnessMs");
        private static final Distribution CUSTOM_PUBLISH_TIME_INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME + "_customPtInputDataFreshnessMs");
        private static final Distribution CUSTOM_EVENT_TIME_INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME + "_customEtInputDataFreshnessMs");
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd--HH-mm");

        // OOM simulation - prepare job update curl command in advance to have it ready in case OOM happens and avoid doing it in the middle of OOM
        private static String jobUpdateCurlCommand = getJobUpdateCurlCommand();

        @ProcessElement
        public void process(@Element PubsubMessage pubsubMessage, @Timestamp Instant timestamp, OutputReceiver<GenericRecord> outputReceiver, BoundedWindow window) {
            try {
                int bigArraySizeInMB = 0;  // put 0 to disable OOM simulation
                byte[] bigArrayToSimulateOOM = createBigArrayToSimulateOOM(bigArraySizeInMB);

                long startMs = System.currentTimeMillis();
                Metrics.counter(CLASS_NAME, "inputRecordCount_" + FORMATTER.print(startMs)).inc();
                PUBSUB_MESSAGE_SIZE_BYTES.update(pubsubMessage.getPayload().length);
                // LOGGER.info("experiments={}", ((DataflowPipelineOptions) ctx.getPipelineOptions()).getExperiments());
                long inputDataFreshnessMs = startMs - timestamp.getMillis();
                String publishTimeAttribute = pubsubMessage.getAttribute(PUBLISH_TIME_ATTRIBUTE);
                String eventTimeAttribute = pubsubMessage.getAttribute(EVENT_TIME_ATTRIBUTE);
                long customPublishTimeInputDataFreshnessMs = publishTimeAttribute != null ? (startMs - Instant.parse(publishTimeAttribute).getMillis()) : -1;
                long customEventTimeInputDataFreshnessMs = eventTimeAttribute != null ? (startMs - Instant.parse(eventTimeAttribute).getMillis()) : -1;

                GenericData.Record record = doProcess(pubsubMessage, inputDataFreshnessMs, customPublishTimeInputDataFreshnessMs, customEventTimeInputDataFreshnessMs);
                String windowString = window instanceof GlobalWindow ? "GlobalWindow " + window.maxTimestamp() : window.toString();
                LOGGER.info("[{}][{}] record {} window {} {}", getIp(), getThreadNameAndId(), record, windowString, getRuntimeInfo());
                outputReceiver.output(record);

                if (inputDataFreshnessMs > 0) INPUT_DATA_FRESHNESS_MS.update(inputDataFreshnessMs);
                if (customPublishTimeInputDataFreshnessMs > 0)
                    CUSTOM_PUBLISH_TIME_INPUT_DATA_FRESHNESS_MS.update(customPublishTimeInputDataFreshnessMs);
                if (customEventTimeInputDataFreshnessMs > 0)
                    CUSTOM_EVENT_TIME_INPUT_DATA_FRESHNESS_MS.update(customEventTimeInputDataFreshnessMs);

                // OOM simulation - so data object is not removed by GC before OOM happens and the log with memory info is printed
                if (bigArrayToSimulateOOM.length > 0) {
                    LOGGER.info("MEMORY size: {}, total memory: {}, free memory: {}", bigArrayToSimulateOOM.length, Runtime.getRuntime().totalMemory(), Runtime.getRuntime().freeMemory());
                }

                long endTimeMs = System.currentTimeMillis();
                PROCESSING_TIME_MS.update(endTimeMs - startMs);

            } catch (Throwable t) {
                Throwable rootCause = getRootCause(t);
                if (t instanceof OutOfMemoryError || isUserCodeExceptionWithOomAsRootCause(t)) {
                    String currentMachineType = getMachineType();
                    if (jobUpdateCurlCommand == null) {
                        jobUpdateCurlCommand = getJobUpdateCurlCommand();
                    }
                    LOGGER.error("Rethrowing OutOfMemoryError. Update the machine type from: {} to: {} by executing job update curl command: \n{}",
                            currentMachineType, getRecommendedHighMemMachineType(currentMachineType), jobUpdateCurlCommand, t);
                    throw t;
                } else if (t instanceof Exception) {
                    LOGGER.error("Exception occurred during processing. Skipping rethrowing it to avoid pipeline indefinite retries. Exception: {}, Root cause: {}", t.getMessage(), rootCause.getMessage(), t);
                } else {
                    LOGGER.error("Rethrowing non OutOfMemoryError throwable: {}, Root cause: {}", t.getMessage(), rootCause.getMessage(), t);
                    throw t;
                }
            }
        }

        private static byte[] createBigArrayToSimulateOOM(int arraySizeInMB) {
            if (arraySizeInMB == 0) {
                return new byte[0];
            }
            return new byte[new Random().nextInt(arraySizeInMB - 2, arraySizeInMB + 2) * 1024 * 1024];
        }

        private GenericData.Record doProcess(PubsubMessage pubsubMessage, long inputDataFreshnessMs, long customPublishTimeInputDataFreshnessMs, long customEventTimeInputDataFreshnessMs) {
            String body = new String(pubsubMessage.getPayload());
            GenericData.Record record = new GenericData.Record(SCHEMA);
            String value = "body=" + body + ", attrs=" + new TreeMap<>(pubsubMessage.getAttributeMap()) + ", msgId=" + pubsubMessage.getMessageId()
                    + ", inputDataFreshnessMs=" + inputDataFreshnessMs + ", customInputDataFreshnessMs=" + customPublishTimeInputDataFreshnessMs
                    + ", customEventTimeInputDataFreshnessMs=" + customEventTimeInputDataFreshnessMs;
            record.put(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID, value);
            return record;
        }
    }

    private static Throwable getRootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static boolean isUserCodeExceptionWithOomAsRootCause(Throwable t) {
        // UserCodeException is always present in dependencies for all maven profiles in this project (via beam-sdks-java-core)
        if (t == null) return false;
        if (t instanceof UserCodeException) {
            Throwable cause = t.getCause();
            while (cause != null && cause != t) {
                if (cause instanceof OutOfMemoryError) {
                    return true;
                }
                cause = cause.getCause();
            }
        }
        return false;
    }

    static class CustomWriteFileNaming implements FileIO.Write.FileNaming {
        private static final DateTimeFormatter FORMATTER_MIN_SECS = DateTimeFormat.forPattern("mm-ss.SSS");
        private static final Logger LOGGER = LoggerFactory.getLogger(CustomWriteFileNaming.class);

        private final String parentAndFormattedDateTimePath;
        private final String compression;
        private final String format;

        public CustomWriteFileNaming(String parentAndFormattedDateTimePath, String compression, String format) {
            this.parentAndFormattedDateTimePath = parentAndFormattedDateTimePath;
            this.compression = compression;
            this.format = format;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            String normalizedWindow = WindowUtils.windowToNormalizedString(window);
            Instant now = Instant.now();
            String path = String.format("%s" +
                            "/p-%s-%s-w-%s" +
                            "-s-%s-of-%s" +
                            "-t-%s" +
                            "-n-%s" +
                            "-i-%s" +
                            "-m_%s" +
                            "-u-%s" +
                            "_%s.%s",
                    parentAndFormattedDateTimePath,
                    pane.getIndex(), pane.getTiming(), normalizedWindow,
                    shardIndex, numShards,
                    Thread.currentThread().threadId(),
                    Thread.currentThread().getName(),
                    getLocalHostAddressSpaced(),
                    FORMATTER_MIN_SECS.print(now), UUID.randomUUID(),
                    this.compression, this.format);

            LOGGER.info("[{}][Write] Writing data to '{}',w={},p={}", ipAddressAndThread(), path, windowToString(window), pane);
            return path;
        }
    }

    @SuppressWarnings("NullableProblems")
    static class ToTimestampedPathKV extends DoFn<GenericRecord, KV<String, GenericRecord>> {

        private static final Logger LOGGER = LoggerFactory.getLogger(ToTimestampedPathKV.class);
        private static final DateTimeFormatter FORMATTER_PATH = DateTimeFormat.forPattern("'/year='yyyy/'month'=MM/'day'=dd/'hour'=HH/'minute'=mm");

        //        private final String outputDir;
        private final ValueProvider<String> outputDir;

        @Override
        public Duration getAllowedTimestampSkew() {
            return Duration.millis(Long.MAX_VALUE);
        }

        //        public ToTimestampedPathKV(String outputDir) {
        public ToTimestampedPathKV(ValueProvider<String> outputDir) {
            this.outputDir = outputDir;
        }

        @ProcessElement
        public void process(@Element GenericRecord record, @Timestamp Instant timestamp, OutputReceiver<KV<String, GenericRecord>> outputReceiver) {
//            String timestampedPath = outputDir + FORMATTER_PATH.print(timestamp);
            String timestampedPath = outputDir.get() + FORMATTER_PATH.print(timestamp);
            LOGGER.info("[{}][{}] timestampedPath={}", getIp(), getThreadNameAndId(), timestampedPath);
            outputReceiver.output(KV.of(timestampedPath, record));
        }
    }

    private static String firstNonNullOrDefaultToEmpty(String value) {
        return value != null && !value.isEmpty() ? value : "";
    }

}

/*
41 threads (in main Thread group) with worker harness 8

AfterPubsub_threadId_Thread-26:102	1
AfterPubsub_threadId_Thread-30:118	10
AfterPubsub_threadId_Thread-31:121	5
AfterPubsub_threadId_Thread-33:126	20
AfterPubsub_threadId_Thread-37:141	23
...
AfterPubsub_threadId_Thread-73:812  11


177 threads with worker harness 64

 */
