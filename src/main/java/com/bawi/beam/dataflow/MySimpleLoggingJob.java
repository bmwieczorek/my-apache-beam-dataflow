package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bawi.beam.dataflow.MySimpleLoggingJob.MyDoFn.THREAD_IDS;

public class MySimpleLoggingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySimpleLoggingJob.class);

/*

#Machine type	vCPUs	Memory	Price (USD)	Preemptible price (same for certral,east,west-1)
#e2-small	    2	    2GB	    $0.016751	$0.005025
#g1-small	    0.5	    1.70GB	$0.0230084	$0.0048439.  Running Dataflow jobs with shared-core instance types (g1-small, f1-micro) is not officially supported.
#n1-standard-1	1	    3.75GB	$0.04749975	$0.01


GCP_BUCKET=${GCP_PROJECT}-${GCP_OWNER}-mysimpleloggingjob
gsutil -q ls -d gs://${GCP_BUCKET} || if [ $? -ne 0 ]; then gsutil mb gs://${GCP_BUCKET}; fi
mvn compile -DskipTests -Pdataflow-runner exec:java \
-Dexec.mainClass=com.bawi.beam.dataflow.MySimpleLoggingJob \
-Dexec.args=" \
  --runner=DataflowRunner \
  ${GCP_JAVA_DATAFLOW_RUN_OPTS} \
  --workerMachineType=e2-small \
  --workerDiskType=compute.googleapis.com/projects/${GCP_PROJECT}/zones/${GCP_ZONE}/diskTypes/pd-standard \
  --diskSizeGb=30 \
  --stagingLocation=gs://${GCP_BUCKET}/staging"

*/

    static class MyDoFn extends DoFn<Integer,Integer> {
        static Set<Long> THREAD_IDS = new HashSet<>();
        private final Distribution bundleSizeDistribution = Metrics.distribution(MyDoFn.class, "bundleSizeDist");

        private Integer bundleSize;

        @StartBundle
        public void startBundle() {
            bundleSize = 0;
        }

        @ProcessElement
        public void process(@Element Integer i, OutputReceiver<Integer> receiver) {
            bundleSize++;
            long id = Thread.currentThread().getId();
            THREAD_IDS.add(id);
            LOGGER.info("[tid={}] Processing: {}", id, i);
            receiver.output(i);
        }

        @FinishBundle
        public void finishBundle() {
            LOGGER.info("[tid={}] Bundle size is {}", Thread.currentThread().getId(), bundleSize);
            bundleSizeDistribution.update(bundleSize);
            bundleSize=0;

        }
    }
    public static void main(String[] args) {
//        args = DataflowUtils.updateDataflowArgs(args);
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
        List<Integer> ints = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        pipeline.apply(Create.of(ints))
                .apply(ParDo.of(new MyDoFn()))
//                .apply(MapElements.into(TypeDescriptors.integers()).via(i -> {
//                    LOGGER.info("[tid={}] Processing: {}", Thread.currentThread().getId(), i);
//                    return i;
//                }))
        ;
        PipelineResult result = pipeline.run();
        result.metrics().allMetrics().getDistributions().forEach(m -> LOGGER.info("{}: {}", m.getName(), m.getCommitted()));
        LOGGER.info("{} thread ids: {}", THREAD_IDS.size(), THREAD_IDS);  // Math.max(Runtime.getRuntime().availableProcessors(), 3)
        // usually waitUntilFinish while pipeline development, remove when generating dataflow classic template
        //result.waitUntilFinish();
    }
}
