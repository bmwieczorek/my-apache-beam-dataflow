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
  --diskSizeGb=20 \
  --stagingLocation=gs://${GCP_BUCKET}/staging"

*/

    static class MyDoFn extends DoFn<Integer,Integer> {
        private final Distribution bundleSizeDistribution = Metrics.distribution(MyDoFn.class, "bundleSizeDist");
        static Set<Long> THREAD_IDS = new HashSet<>();

        private int bundleSize;

        @StartBundle
        public void startBundle() {
            bundleSize = 0;
        }

        @ProcessElement
        public void process(@Element Integer i, OutputReceiver<Integer> receiver) {
            bundleSize++;
            THREAD_IDS.add(Thread.currentThread().getId());
            LOGGER.info("[tid={}] Processing: {}", Thread.currentThread().getId(), i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
        List<Integer> ints = IntStream.rangeClosed(1, 100).boxed().collect(Collectors.toList());
        pipeline.apply(Create.of(ints))
                .apply(ParDo.of(new MyDoFn()))
//                .apply(MapElements.into(TypeDescriptors.integers()).via(i -> {
//                    LOGGER.info("[tid={}] Processing: {}", Thread.currentThread().getId(), i);
//                    return i;
//                }))
        ;
        PipelineResult result = pipeline.run();
        if ("DirectPipelineResult".equals(result.getClass().getSimpleName())) {
            result.metrics().allMetrics().getDistributions().forEach(m -> LOGGER.info("{}: {}", m.getName(), m.getCommitted()));
            LOGGER.info("{} thread ids: {}", THREAD_IDS.size(), THREAD_IDS);  // Math.max(Runtime.getRuntime().availableProcessors(), 3)
            result.waitUntilFinish(); // usually waitUntilFinish while pipeline development, remove when generating dataflow classic template
        }

        // direct runner - 12 worker threads at 12 logical cpu cores on Mac, for 100 elements 26 bundles with size between 2 and 4
//        2022-09-15 15:53:14,144 [direct-runner-workerid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:73 - [tid=22] Bundle size is 4
//        2022-09-15 15:53:14,145 [direct-runner-workerid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:73 - [tid=24] Bundle size is 4
//        2022-09-15 15:53:14,148 [direct-runner-workerid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:62 - [tid=15] Processing: 99
//        2022-09-15 15:53:14,251 [direct-runner-workerid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:62 - [tid=15] Processing: 100
//        2022-09-15 15:53:14,353 [direct-runner-workerid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:73 - [tid=15] Bundle size is 2
//        2022-09-15 15:53:14,397 [mainid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:92 - com.bawi.beam.dataflow.MySimpleLoggingJob$MyDoFn:bundleSizeDist: DistributionResult{sum=100, count=26, min=2, max=4}
//        2022-09-15 15:53:14,398 [mainid] INFO  com.bawi.beam.dataflow.MySimpleLoggingJob:93 - 12 thread ids: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 15]

        // dataflow runner with workerMachineType with e2-small with 2 cores
//        Info 2022-09-15 08:00:39.480 UTC Splitting source [0, 100) produced 1 bundles with total serialized response size 1679
//        Info 2022-09-15 08:00:48.393 UTC [tid=30] Bundle size is 74
//        Info 2022-09-15 08:00:51.098 UTC [tid=29] Bundle size is 26
//        Counter name            Value
//        bundleSizeDist_COUNT	2
//        bundleSizeDist_MAX	    74
//        bundleSizeDist_MEAN	    50
//        bundleSizeDist_MIN	    26

    }


}
