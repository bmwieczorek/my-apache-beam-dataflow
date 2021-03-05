package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySimpleLoggingDataflowJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySimpleLoggingDataflowJob.class);

/*

PROJECT=$(gcloud config get-value project)
BUCKET=${PROJECT}-$USER-mysimpleloggingdataflowjob
gsutil mb gs://${BUCKET}

mvn clean package -DskipTests exec:java \
-Pdataflow-runner \
-Dexec.mainClass=com.bawi.beam.dataflow.MySimpleLoggingDataflowJob \
-Dexec.args=" \
  --runner=DataflowRunner \
  --project=${PROJECT} ${JAVA_DATAFLOW_RUN_OPTS} \
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
        result.waitUntilFinish();
    }
}
