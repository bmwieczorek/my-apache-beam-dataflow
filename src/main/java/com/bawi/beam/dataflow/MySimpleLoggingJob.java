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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bawi.beam.dataflow.MySimpleLoggingJob.MyFn.THREAD_IDS;

public class MySimpleLoggingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySimpleLoggingJob.class);

/*

#Machine type	vCPUs	Memory	Price (USD)	Preemptible price (same for certral,east,west-1)
#e2-small	    2	    2GB	    $0.016751	$0.005025
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
  --numWorkers=2 \
  --maxNumWorkers=2 \
  --diskSizeGb=25 \
  --stagingLocation=gs://${GCP_BUCKET}/staging"

*/

    static class MyFn extends DoFn<Integer,Integer> {
        private final Distribution bundleSizeDistribution = Metrics.distribution(MyFn.class, "bundleSizeDist");
        static Set<Long> THREAD_IDS = new HashSet<>();

        private int bundleSize;

        @StartBundle
        public void startBundle() {
            bundleSize = 0;
            LOGGER.info("[{}][tid={}] Starting bundle", getIP(), Thread.currentThread().getId());
        }

        @ProcessElement
        public void process(@Element Integer i, OutputReceiver<Integer> receiver) {
            bundleSize++;
            THREAD_IDS.add(Thread.currentThread().getId());
            LOGGER.info("[{}][tid={}] Processing: {}", getIP(), Thread.currentThread().getId(), i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            receiver.output(i);
        }

        @FinishBundle
        public void finishBundle() {
            LOGGER.info("[{}][tid={}] Bundle size is {}", getIP(), Thread.currentThread().getId(), bundleSize);
            bundleSizeDistribution.update(bundleSize);
            bundleSize=0;

        }
    }
    public static void main(String[] args) {
//        args = DataflowUtils.updateDataflowArgs(args);
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());
        List<Integer> ints = IntStream.rangeClosed(1, 200).boxed().collect(Collectors.toList());
        pipeline.apply(Create.of(ints))
                .apply(ParDo.of(new MyFn()))
//                .apply(MapElements.into(TypeDescriptors.integers()).via(i -> {
//                    LOGGER.info("[{}][tid={}] Processing: {}", getLocalHostAddress(), Thread.currentThread().getId(), i);
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
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=25] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=19] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=22] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=15] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=21] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=23] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=20] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=14] Starting bundle
//        14:16:12,259 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=18] Starting bundle
//        14:16:12,266 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=14] Processing: 133
//        14:16:12,266 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=23] Processing: 177
//        ...
//        14:16:13,267 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=14] Processing: 134
//        14:16:13,267 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=23] Processing: 178
//        ...
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=20] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=18] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=25] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=22] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=15] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=19] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=14] Bundle size is 11
//        14:16:23,296 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=21] Bundle size is 11
//        14:16:24,298 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=23] Bundle size is 12
//        ...
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=19] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=15] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=23] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=20] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=14] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=16] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=20] Processing: 34
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=24] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=17] Starting bundle
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=23] Processing: 122
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=15] Processing: 56
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:59 - [127.0.0.1][tid=25] Starting bundle
//        ...
//        14:16:24,308 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=14] Processing: 144
//        14:16:25,312 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=23] Processing: 123
//        ...
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=20] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:66 - [127.0.0.1][tid=19] Processing: 200
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=25] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=24] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=23] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=14] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=16] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=17] Bundle size is 11
//        14:16:35,336 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=15] Bundle size is 11
//        14:16:36,341 [direct-runner-workerid] INFO  MySimpleLoggingJob$MyFn:77 - [127.0.0.1][tid=19] Bundle size is 12
//        14:16:36,385 [mainid] INFO  MySimpleLoggingJob:96 - com.bawi.beam.dataflow.MySimpleLoggingJob$MyFn:bundleSizeDist: DistributionResult{sum=200, count=18, min=11, max=12}
//        14:16:36,385 [mainid] INFO  MySimpleLoggingJob:97 - 12 thread ids: [16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 14, 15]



        // dataflow runner with 2 workers and each workerMachineType with e2-small with 2 cores:
//        11:57:02.728 [10.128.0.21][tid=29] Starting bundle
//        11:57:02.732 [10.128.0.21][tid=29] Processing: 1
//        11:57:03.747 [10.128.0.21][tid=29] Processing: 2
//        ...
//        11:57:14.785 [10.128.0.21][tid=29] Processing: 13
//        11:57:14.864 [10.128.0.21][tid=30] Starting bundle
//        11:57:14.864 [10.128.0.21][tid=30] Processing: 102
//        11:57:15.786 [10.128.0.21][tid=29] Processing: 14
//        11:57:15.864 [10.128.0.21][tid=30] Processing: 103
//        ...
//        11:57:22.900 [10.128.0.22][tid=29] Starting bundle
//        11:57:22.903 [10.128.0.22][tid=29] Processing: 56
//        11:57:23.809 [10.128.0.21][tid=29] Processing: 22
//        ...
//        11:57:25.871 [10.128.0.21][tid=30] Processing: 113
//        11:57:25.906 [10.128.0.22][tid=29] Processing: 59
//        11:57:25.950 [10.128.0.22][tid=30] Starting bundle
//        11:57:25.951 [10.128.0.22][tid=30] Processing: 153
//        11:57:26.811 [10.128.0.21][tid=29] Processing: 25
//        ...
//        11:57:37.913 [10.128.0.21][tid=29] Bundle size is 35
//        ...
//        11:57:39.101 [10.128.0.21][tid=29] Starting bundle
//        ...
//        11:57:44.890 [10.128.0.21][tid=30] Bundle size is 30
//        ...
//        11:57:45.980 [10.128.0.21][tid=30] Starting bundle
//        ...
//        11:58:00.145 [10.128.0.21][tid=29] Bundle size is 21
//        ...
//        11:58:06.009 [10.128.0.21][tid=30] Bundle size is 20
//        ...
//        11:58:09.016 [10.128.0.22][tid=29] Bundle size is 46
//        ...
//        11:58:14.023 [10.128.0.22][tid=30] Bundle size is 48
//

//        Counter name 		    Value	Step
//        bundleSizeDist_COUNT	6	    ParDo(MyFn)
//        bundleSizeDist_MAX	48	    ParDo(MyFn)
//        bundleSizeDist_MEAN	33	    ParDo(MyFn)
//        bundleSizeDist_MIN	2	    ParDo(MyFn)

    }

    private static String getIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }
}
