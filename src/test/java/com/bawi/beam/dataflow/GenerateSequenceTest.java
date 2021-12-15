package com.bawi.beam.dataflow;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;

public class GenerateSequenceTest {
    public static void main(String[] args) {
        //PipelineOptions options = PipelineOptionsFactory.create();
        PipelineOptions options = createDataflowRunnerOptions();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(GenerateSequence.from(1).to(60).withRate(1, Duration.standardSeconds(1)))
                .apply(MyConsoleIO.write());
        PipelineResult run = pipeline.run();
        //run.waitUntilFinish();
    }

    private static PipelineOptions createDataflowRunnerOptions() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(System.getenv("GCP_PROJECT"));
        options.setRegion(System.getenv("GCP_REGION"));
        options.setServiceAccount(System.getenv("GCP_SERVICE_ACCOUNT"));
        options.setNetwork(System.getenv("GCP_NETWORK"));
        options.setSubnetwork(System.getenv("GCP_SUBNETWORK").equals("default") ? null : System.getenv("GCP_SUBNETWORK"));
        options.setUsePublicIps(false);
        options.setRunner(DataflowRunner.class);
        options.setStagingLocation("gs://" + System.getenv("GCP_PROJECT") + "/staging");
        return options;
    }
}
