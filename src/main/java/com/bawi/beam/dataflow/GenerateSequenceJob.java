package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;

import static com.bawi.beam.dataflow.IamUtils.addPubSubAdminRoleToServiceAccount;
import static com.bawi.beam.dataflow.PubSubUtils.createTopicAndSubscription;
import static com.bawi.beam.dataflow.PubSubUtils.deleteSubscriptionAndTopic;

public class GenerateSequenceJob {
    private static final String PROJECT = System.getenv("GCP_PROJECT");
    public static final String OWNER = System.getenv("GCP_OWNER");
    private static final String BUCKET = PROJECT + "-" + OWNER;
    private static final String JOB_NAME = OWNER + "-generate-tps";
    private static final String TOPIC_NAME = OWNER + "-" + "topic";
    private static final String TOPIC = "projects/" + PROJECT + "/topics/" + TOPIC_NAME;
    private static final String SUBSCRIPTION_NAME = TOPIC_NAME + "-sub";
    private static final String SERVICE_ACCOUNT = System.getenv("GCP_SERVICE_ACCOUNT");
    private static final String SUBNETWORK = System.getenv("GCP_SUBNETWORK");
    private static final int TPS = 100;

    public static void main(String[] args) throws Exception {
        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
        createTopicAndSubscription(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
        addPubSubAdminRoleToServiceAccount(PROJECT, TOPIC_NAME, SERVICE_ACCOUNT, "roles/pubsub.admin");

        args = new String[]{
                "--region=" + System.getenv("GCP_REGION"),
                "--runner=DataflowRunner",

                "--numWorkers=1",
                "--maxNumWorkers=5",

                "--usePublicIps=false",
                "--workerMachineType=t2d-standard-1",
                "--experiments=enable_stackdriver_agent_metrics",

                "--jobName=" + JOB_NAME,

                "--dataflowServiceOptions=enable_streaming_engine_resource_based_billing",
                "--enableStreamingEngine",

                "--stagingLocation=gs://" + BUCKET + "/" + JOB_NAME + "/staging",
                "--gcpTempLocation=gs://" + BUCKET + "/" + JOB_NAME + "/temp",
                "--project=" + PROJECT,
                "--serviceAccount=" + SERVICE_ACCOUNT,
                "--subnetwork=" + SUBNETWORK
        };

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(org.apache.beam.sdk.io.GenerateSequence
                    .from(0)
                    .to(60 * 30 * TPS) // so send messages for 30 mins we need 100 tps *60 secs in min *30 min = 180 000
                    .withRate(TPS, Duration.standardSeconds(1L)) // send each number every 10ms (1/100s)
                )
                .apply(ParDo.of(new CreatePubsubMessageFn()))
                .apply(PubsubIO.writeMessages().to(TOPIC));
        PipelineResult run = pipeline.run();
        run.waitUntilFinish();

        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    static class CreatePubsubMessageFn extends DoFn<Long, PubsubMessage> {
        private static final Map<String, String> MAP = Map.of("key1", "value1" , "key2", "value2" );
        private static final List<Integer> weights = List.of(80, 85, 90, 95, TPS, 95, 90, 85, 80, 75, 70, 65, 60, 65, 70);

        @ProcessElement
        public void processElement(@Element Long number, OutputReceiver<PubsubMessage> outputReceiver) {
            int weight = weights.get(number.intValue() / (5 * 60 * TPS));
            for (int i = 0; i < weight; i++) {
                outputReceiver.output(new PubsubMessage((number + "_" + i).getBytes(), MAP));
            }
        }
    }
}

