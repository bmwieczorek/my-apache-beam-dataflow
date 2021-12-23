package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.MoreObjects.firstNonNull;


public class MyPubsubToGCSJob {


    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        String getSubscription();

        void setSubscription(String value);

        @Validation.Required
        String getOutput();

        void setOutput(String value);

        // DataflowPipelineOptions
//        String getTemplateLocation();
//        void setTemplateLocation(String value);
//
//        int getDiskSizeGb();
//        void setDiskSizeGb(int value);
//
//        int getNumWorkers();
//        void setNumWorkers(int value);
//
//        int getMaxNumWorkers();
//        void setMaxNumWorkers(int value);
//
//        String getWorkerMachineType();
//        void setWorkerMachineType(String value);
//
//        String getWorkerDiskType();
//        void setWorkerDiskType(String value);
//
//        String getSubnetwork();
//        void setSubnetwork(String value);
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
 --additional-experiments use_network_tags=default-uscentral1 \
 --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-standard,diskSizeGb=200,subscription=projects/$PROJECT/subscriptions/messages


# --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-ssd,diskSizeGb=200,workerLogLevelOverrides={\"org.apache.kafka.clients.consumer.internals.Fetcher\":\"WARN\"},subscription=projects/$PROJECT/subscriptions/messages

    */

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSJob.class);

    static final String BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID = "bodyWithAttributesMessageId";
    private static final Schema SCHEMA = SchemaBuilder.record("record").fields().requiredString(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID).endRecord();
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/'minute'=mm");



    static class ConcatBodyAttrAndMsgIdFn extends DoFn<PubsubMessage, GenericRecord> {
        private static final String CLASS_NAME = ConcatBodyAttrAndMsgIdFn.class.getSimpleName();
        private static final Distribution PUBSUB_MESSAGE_SIZE_BYTES = Metrics.distribution(CLASS_NAME, CLASS_NAME +"_pubsubMessageSizeBytes");
        private static final Distribution PROCESSING_TIME_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME +"_processingTimeMs");
        private static final Distribution INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME +"_inputDataFreshnessMs");
        private static final Distribution CUSTOM_PUBLISH_TIME_INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME +"_customPtInputDataFreshnessMs");
        private static final Distribution CUSTOM_EVENT_TIME_INPUT_DATA_FRESHNESS_MS = Metrics.distribution(CLASS_NAME, CLASS_NAME +"_customEtInputDataFreshnessMs");
        static final String PUBLISH_TIME_ATTRIBUTE = "pt";
        static final String EVENT_TIME_ATTRIBUTE = "et";

        @ProcessElement
        public void process(@Element PubsubMessage pubsubMessage, @Timestamp Instant timestamp, OutputReceiver<GenericRecord> outputReceiver, BoundedWindow window) {
            long startMs = System.currentTimeMillis();
            PUBSUB_MESSAGE_SIZE_BYTES.update(pubsubMessage.getPayload() != null ? pubsubMessage.getPayload().length : 0);

            long inputDataFreshnessMs = startMs - timestamp.getMillis();
            String publishTimeAttribute = pubsubMessage.getAttribute(PUBLISH_TIME_ATTRIBUTE);
            String eventTimeAttribute = pubsubMessage.getAttribute(EVENT_TIME_ATTRIBUTE);
            long customPublishTimeInputDataFreshnessMs = publishTimeAttribute != null ? (startMs - Instant.parse(publishTimeAttribute).getMillis()) : -1;
            long customEventTimeInputDataFreshnessMs = eventTimeAttribute != null ? (startMs - Instant.parse(eventTimeAttribute).getMillis()) : -1;

            GenericData.Record record = doProcess(pubsubMessage, inputDataFreshnessMs, customPublishTimeInputDataFreshnessMs, customEventTimeInputDataFreshnessMs);
            String windowString = window instanceof GlobalWindow ? "GlobalWindow " + window.maxTimestamp() : window.toString();
            LOGGER.info("record {} window {} {}", record, windowString, getMessage());
            outputReceiver.output(record);

            if (inputDataFreshnessMs > 0) INPUT_DATA_FRESHNESS_MS.update(inputDataFreshnessMs);
            if (customPublishTimeInputDataFreshnessMs > 0) CUSTOM_PUBLISH_TIME_INPUT_DATA_FRESHNESS_MS.update(customPublishTimeInputDataFreshnessMs);
            if (customEventTimeInputDataFreshnessMs > 0) CUSTOM_EVENT_TIME_INPUT_DATA_FRESHNESS_MS.update(customEventTimeInputDataFreshnessMs);

            long endTimeMs = System.currentTimeMillis();
            PROCESSING_TIME_MS.update(endTimeMs - startMs);
        }

        private GenericData.Record doProcess(PubsubMessage pubsubMessage, long inputDataFreshnessMs, long customPublishTimeInputDataFreshnessMs, long customEventTimeInputDataFreshnessMs) {
            String body = new String(pubsubMessage.getPayload());
            GenericData.Record record = new GenericData.Record(SCHEMA);
            String value = "body=" + body + ", attributes=" + new TreeMap<>(pubsubMessage.getAttributeMap()) + ", messageId=" + pubsubMessage.getMessageId()
                    + ", inputDataFreshnessMs=" + inputDataFreshnessMs + ", customInputDataFreshnessMs=" + customPublishTimeInputDataFreshnessMs
                    + ", customEventTimeInputDataFreshnessMs=" + customEventTimeInputDataFreshnessMs;
            record.put(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID, value);
            return record;
        }
    }

    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(options);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        String output = options.getOutput();
        readingPipeline
//                .apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription()).withIdAttribute("myMsgAttrName")) // deduplicated based on myMsgAttrName
                .apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription()).withTimestampAttribute(ConcatBodyAttrAndMsgIdFn.EVENT_TIME_ATTRIBUTE))
                .apply(Window.<PubsubMessage>into(FixedWindows.of(Duration.standardSeconds(60))).withAllowedLateness(Duration.standardMinutes(10)).discardingFiredPanes()) // if Window.into is after DoFn then DoFn logs Global Window otherwise IntervalWindow
                .apply(ConcatBodyAttrAndMsgIdFn.CLASS_NAME, ParDo.of(new ConcatBodyAttrAndMsgIdFn()))
                .setCoder(AvroGenericCoder.of(SCHEMA)) // required to explicitly set coder for GenericRecord
                .apply(AvroIO.writeGenericRecords(SCHEMA).withWindowedWrites()
                        //.to(options.getOutput())
                        .to(new FileBasedSink.FilenamePolicy() {

                            @Override
                            public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
                                ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(output);
                                IntervalWindow intervalWindow = (IntervalWindow) window;
                                String parentDirectoryPath = resource.isDirectory() ? "" : firstNonNull(resource.getFilename(), "");
                                String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
                                String suffix = suggestedFilenameSuffix != null && !suggestedFilenameSuffix.isEmpty() ? suggestedFilenameSuffix : ".avro";
                                PaneInfo.Timing pathTiming = paneInfo.getTiming();
                                String yyyyMMddHHmm = FORMATTER.print(intervalWindow.start());
                                String filename = String.format("%s/%s/%s-%s-%s-%s-%s-of-%s%s", parentDirectoryPath, yyyyMMddHHmm, System.currentTimeMillis(), UUID.randomUUID(), window.maxTimestamp().toString().replace(":","_").replace(" ","_"), pathTiming, shardNumber, numShards, suffix);
//                                LOGGER.info("filename='{}', {}", filename, getMessage());  // output/year=2021/month=12/day=23/hour=07/minute=52/1640246141797-d52a2109-f8f9-459b-b055-365be2558833-2021-12-23T07_52_59.999Z-ON_TIME-1-of-4.avro
                                return resource.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
                            }

                            @Override
                            public @Nullable ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
                                throw new UnsupportedOperationException("Unsupported.");
                            }
                        })
                        .withTempDirectory(FileBasedSink.convertToFileResourceIfPossible(output).getCurrentDirectory())
                        .withNumShards(4));

        readingPipeline.run();
    }

    private static String getMessage() {
        InetAddress localHostAddress = getLocalHostAddress();
        Thread thread = Thread.currentThread();
        String total = format(Runtime.getRuntime().totalMemory());
        String free = format(Runtime.getRuntime().freeMemory());
        String used = format(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        String max = format(Runtime.getRuntime().maxMemory());
        return String.format("%s|i:%s|n:%s|g:%s|c:%s|u:%s|f:%s|t:%s|m:%s",
                localHostAddress, thread.getId(), thread.getName(), thread.getThreadGroup().getName(), Runtime.getRuntime().availableProcessors(), used, free, total, max);
    }

    private static InetAddress getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }

    private static String format(long value) {
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(true);
        return numberFormat.format(value);
    }
}
