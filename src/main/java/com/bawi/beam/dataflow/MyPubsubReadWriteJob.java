package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MyPubsubReadWriteJob {
    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getTopic();
        void setTopic(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getSubscription();
        void setSubscription(ValueProvider<String> value);
    }

/*

PROJECT=$(gcloud config get-value project)
OWNER=bartek
JOB=mypubsubreadwritejob
BUCKET=${PROJECT}-$OWNER-${JOB}
gsutil mb gs://${BUCKET}
gcloud pubsub subscriptions delete $OWNER-${JOB}-sub
gcloud pubsub topics delete $OWNER-${JOB}
gcloud pubsub topics create $OWNER-${JOB}
gcloud pubsub subscriptions create $OWNER-${JOB}-sub --topic=$OWNER-${JOB}


mvn clean compile -DskipTests -Pdataflow-runner exec:java \
-Dexec.mainClass=com.bawi.beam.dataflow.MyPubsubReadWriteJob\$Write \
-Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --jobName=${JOB}-write-$OWNER \
  --topic=projects/${PROJECT}/topics/$OWNER-${JOB} \
  --experiments=enable_stackdriver_agent_metrics \
  --labels='{ \"my_job_name\" : \"${JOB}-write-$OWNER\"}'"

*/

    public static class Write {
        public static void main(String[] args) {
            MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
            Pipeline writingPipeline = Pipeline.create(options);

            List<String> strings = IntStream.rangeClosed(1, 100000).mapToObj(String::valueOf).collect(Collectors.toList());
            writingPipeline.apply(Create.of(strings))
                    .apply(ParDo.of(new CreatePubsubMessageFn()))

                    // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
                    .apply(PubsubIO.writeMessages().to(options.getTopic()));

            writingPipeline.run();
        }

        private static class CreatePubsubMessageFn extends DoFn<String, PubsubMessage> {
            private static final Map<String, String> MAP = Stream.of(new String[][]{{"key1", "value1"}, {"key2", "value2"}})
                    .collect(Collectors.toMap(kvArr -> kvArr[0], kvArr -> kvArr[1]));

            @ProcessElement
            public void processElement(@Element String word, OutputReceiver<PubsubMessage> outputReceiver) {
                outputReceiver.output(new PubsubMessage(word.getBytes(), MAP));
            }
        }
    }

/*

PROJECT=$(gcloud config get-value project)
OWNER=bartek
JOB=mypubsubreadwritejob
BUCKET=${PROJECT}-$OWNER-${JOB}
gsutil mb gs://${BUCKET}

mvn clean compile -DskipTests exec:java \
-Pdataflow-runner \
-Dexec.mainClass=com.bawi.beam.dataflow.MyPubsubReadWriteJob\$Read \
-Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --topic=projects/${PROJECT}/topics/$OWNER-${JOB} \
  --jobName=${JOB}-read-$OWNER \
  --workerMachineType=n1-standard-4 \
  --maxNumWorkers=5 \
  --subscription=projects/${PROJECT}/subscriptions/$OWNER-${JOB}-sub \
  --experiments=enable_stackdriver_agent_metrics \
  --profilingAgentConfiguration='{ \"APICurated\" : true }' \
  --labels='{ \"my_job_name\" : \"${JOB}-read-$OWNER\"}'"

  --workerMachineType=n1-standard-4 \
*/

    public static class Read {
        private static final Logger LOGGER = LoggerFactory.getLogger(Read.class);

        public static void main(String[] args) {
            args = DataflowUtils.updateDataflowArgs(args);
            MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);

//            DataflowProfilingOptions.DataflowProfilingAgentConfiguration profilingConf = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
//            profilingConf.put("APICurated", true);
//            options.setProfilingAgentConfiguration(profilingConf);
//            options.setStreaming(true);
//            options.setAutoscalingAlgorithm(DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED);

            Pipeline readingPipeline = Pipeline.create(options);

            // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
            readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(options.getSubscription()))
                    .apply(MapElements.via(new SimpleFunction<PubsubMessage, String>() {
                        @Override
                        public String apply(PubsubMessage msg) {
                            int i = Integer.parseInt(new String(msg.getPayload()));
                            String factorial = factorial(i);
                            LOGGER.info("[{}], i={}, fact={}", getMessage(), i, factorial);
                            return "body=" + i +
                                    ", attributes=" + msg.getAttributeMap() +
                                    ", messageId=" + msg.getMessageId();

                        }
                    }))
                    .apply(MyConsoleIO.write());

            readingPipeline.run();
        }

        private static String factorial(int limit) {
            BigInteger factorial = BigInteger.valueOf(1);
            for (int i = 1; i <= limit; i++) {
                factorial = factorial.multiply(BigInteger.valueOf(i));
            }
            return factorial.toString();
        }

        private static String getMessage() {
            InetAddress localHostAddress = getLocalHostAddress();
            Thread thread = Thread.currentThread();
            String total = format(Runtime.getRuntime().totalMemory());
            String free = format(Runtime.getRuntime().freeMemory());
            String used = format(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
            String max = format(Runtime.getRuntime().maxMemory());
            return String.format("%s|i:%s|n:%s|g:%s|c:%s|u:%s|f:%s|t:%s|m:%s",
                    localHostAddress, thread.getId(), thread.getName(), thread.getThreadGroup().getName(), Runtime.getRuntime().availableProcessors(), used, free, total, max);
        }

        private static InetAddress getLocalHostAddress() {
            try {
                return InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                LOGGER.error("Unable to get local host address", e);
                return null;
            }
        }

        private static String format(long value) {
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setGroupingUsed(true);
            return numberFormat.format(value);
        }

    }
}
