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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;

public class MyPubsubToConsoleJob {


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
USER=bartek
BUCKET=$APP-$USER
java11

gsutil mb gs://${BUCKET}
mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=$PROJECT
gsutil cp target/classes/flex-templates/app-image-spec.json gs://${BUCKET}/

gcloud dataflow flex-template run $APP-$USER \
 ${GCLOUD_DATAFLOW_RUN_OPTS} \
 --template-file-gcs-location gs://${BUCKET}/app-image-spec.json  \
 --worker-machine-type n2-standard-2 --num-workers 1  --max-workers 2 \
 --additional-experiments use_network_tags=default-uscentral1 \
 --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-standard,diskSizeGb=200,subscription=projects/$PROJECT/subscriptions/messages


# --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-ssd,diskSizeGb=200,workerLogLevelOverrides={\"org.apache.kafka.clients.consumer.internals.Fetcher\":\"WARN\"},subscription=projects/$PROJECT/subscriptions/messages

    */

    public static final String BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID = "bodyWithAttributesAndMessageId";

    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToConsoleJob.class);
    private static final Schema SCHEMA = SchemaBuilder.record("record").fields().requiredString(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID).endRecord();
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");


    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(options);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        String output = options.getOutput();
        readingPipeline
                .apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription()))
                .apply(MapElements.via(new SimpleFunction<PubsubMessage, GenericRecord>() {
                    @Override
                    public GenericData.Record apply(PubsubMessage msg) {
                        String body = new String(msg.getPayload());
                        LOGGER.info("body {}", body);
                        GenericData.Record record = new GenericData.Record(SCHEMA);
                        record.put(BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID, "body=" + body + ", attributes=" + msg.getAttributeMap() + ", messageId=" + msg.getMessageId());
                        return record;
                    }
                }))
                .setCoder(AvroGenericCoder.of(SCHEMA)) // required to explicitly set coder for GenericRecord
//                .apply(MyConsoleIO.write());
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(60))))
                .apply(AvroIO.writeGenericRecords(SCHEMA).withWindowedWrites()
                        //.to(options.getOutput())
                        .to(new FileBasedSink.FilenamePolicy() {

                            @Override
                            public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
                                ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(output);
                                IntervalWindow intervalWindow = (IntervalWindow) window;
                                String prefix = resource.isDirectory() ? "" : firstNonNull(resource.getFilename(), "");
                                String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
                                String suffix = suggestedFilenameSuffix != null && !suggestedFilenameSuffix.isEmpty() ? suggestedFilenameSuffix : ".avro";
                                String filename =
                                        String.format(
                                                "%s/%s-of-%s%s",
                                                String.format("%s/%s", prefix, FORMATTER.print(intervalWindow.start())),
                                                shardNumber,
                                                numShards,
                                                suffix);

                                LOGGER.info("filename='{}'", filename);
                                return resource
                                        .getCurrentDirectory()
                                        .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
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
}
