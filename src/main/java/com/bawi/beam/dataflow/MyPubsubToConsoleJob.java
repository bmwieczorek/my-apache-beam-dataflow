package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyPubsubToConsoleJob {
    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        String getSubscription();

        void setSubscription(String value);

        // DataflowPipelineOptions
        String getTemplateLocation();
        void setTemplateLocation(String value);

        int getDiskSizeGb();
        void setDiskSizeGb(int value);

        int getNumWorkers();
        void setNumWorkers(int value);

        int getMaxNumWorkers();
        void setMaxNumWorkers(int value);

        String getWorkerMachineType();
        void setWorkerMachineType(String value);

        String getWorkerDiskType();
        void setWorkerDiskType(String value);

        String getSubnetwork();
        void setSubnetwork(String value);
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
 --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-ssd,diskSizeGb=200,subscription=projects/$PROJECT/subscriptions/messages


# --parameters workerDiskType=compute.googleapis.com/projects/$PROJECT/zones/us-central1-c/diskTypes/pd-ssd,diskSizeGb=200,workerLogLevelOverrides={\"org.apache.kafka.clients.consumer.internals.Fetcher\":\"WARN\"},subscription=projects/$PROJECT/subscriptions/messages

    */


    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToConsoleJob.class);

    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline readingPipeline = Pipeline.create(options);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId()
                .fromSubscription(options.getSubscription()))
                .apply(MapElements.via(new SimpleFunction<PubsubMessage, String>() {
                    @Override
                    public String apply(PubsubMessage msg) {
                        String body = new String(msg.getPayload());
                        LOGGER.info("body {}", body);
                        return "body=" + body + ", attributes=" + msg.getAttributeMap() + ", messageId=" + msg.getMessageId();
                    }
                }))
                .apply(MyConsoleIO.write());

        readingPipeline.run();
    }
}
