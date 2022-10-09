package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
Direct runner:
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-runners-direct-java</artifactId>
    <scope>test</scope>
</dependency>

Dataflow runner:
<dependency>
    <groupId>org.apache.beam</groupId>
    <artifactId>beam-runners-google-cloud-dataflow-java</artifactId>
    <scope>runtime</scope>
</dependency>
required args:
--runner=DataflowRunner --project= --region= --tempLocation=gs://
optional args:
--serviceAccount= --network= --subnetwork= --usePublicIps= --workerMachineType= \
--workerDiskType= --numWorkers=2 --maxNumWorkers=2 -stagingLocation=gs://
 */
public class MyLoggingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyLoggingJob.class);

    public static void main(String[] args) {
        args = DataflowUtils.updateDataflowArgs(args);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

// Maven runtime dependency org.apache.beam:
// mvn beam-runners-direct-java and beam-runners-google-cloud-dataflow-java
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Create", Create.of("hello", "jdd", "conf"))
                .apply("Log", MapElements.into(TypeDescriptors.voids()).via(word -> {
                    LOGGER.info(word);
                    return null;
                }));

        pipeline.run();
    }
}

