package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySimpleLoggingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySimpleLoggingJob.class);

/*

#Machine type	vCPUs	Memory	Price (USD)	Preemptible price (same for certral,east,west-1)
#e2-small	    2	    2GB	    $0.016751	$0.005025
#g1-small	    0.5	    1.70GB	$0.0230084	$0.0048439.  Running Dataflow jobs with shared-core instance types (g1-small, f1-micro) is not officially supported.
#n1-standard-1	1	    3.75GB	$0.04749975	$0.01

BUCKET=${PROJECT}-$OWNER-mysimpleloggingjob
gsutil -q ls -d gs://${BUCKET} || if [ $? -ne 0 ]; then gsutil mb gs://${BUCKET}; fi

mvn clean package -DskipTests -Pdataflow-runner exec:java \
-Dexec.mainClass=com.bawi.beam.dataflow.MySimpleLoggingJob \
-Dexec.args=" \
  --runner=DataflowRunner \
  ${JAVA_DATAFLOW_RUN_OPTS} \
  --workerMachineType=e2-small \
  --workerDiskType=compute.googleapis.com/projects/${PROJECT}/zones/${ZONE}/diskTypes/pd-standard \
  --diskSizeGb=30 \
  --stagingLocation=gs://${BUCKET}/staging"

*/

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
        pipeline.apply(Create.of("a", "b", "c"))
                .apply(MapElements.into(TypeDescriptors.strings()).via(s -> {
                    LOGGER.info("[LOGGER] Processing: {}", s);
                    System.out.println("[Console] Processing: " + s);
                    return s;
                }));
        PipelineResult result = pipeline.run();

        // usually waitUntilFinish while pipeline development, remove when generating dataflow classic template
        //result.waitUntilFinish();
    }
}
