package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;

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
USER=bartek
JOB=mypubsubreadwritejob
BUCKET=${PROJECT}-$USER-${JOB}
gsutil mb gs://${BUCKET}
gcloud pubsub subscriptions delete $USER-${JOB}-sub
gcloud pubsub topics delete $USER-${JOB}
gcloud pubsub topics create $USER-${JOB}
gcloud pubsub subscriptions create $USER-${JOB}-sub --topic=$USER-${JOB}


mvn clean compile -DskipTests -Pdataflow-runner exec:java \
-Dexec.mainClass=com.bawi.beam.dataflow.MyPubsubReadWriteJob\$Write \
-Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --jobName=${JOB}-write-$USER \
  --topic=projects/${PROJECT}/topics/$USER-${JOB}"

*/

    public static class Write {
        public static void main(String[] args) {
            MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
            Pipeline writingPipeline = Pipeline.create(options);

            List<String> strings = IntStream.rangeClosed(1, 50000).mapToObj(i -> "a" + i).collect(Collectors.toList());
            writingPipeline.apply(Create.of(strings))
                    .apply(ParDo.of(new CreatePubsubMessageFn()))

                    // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
                    .apply(PubsubIO.writeMessages().to(options.getTopic()));

            PipelineResult result = writingPipeline.run();
//            result.waitUntilFinish();
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
USER=bartek
JOB=mypubsubreadwritejob
BUCKET=${PROJECT}-$USER-${JOB}
gsutil mb gs://${BUCKET}

mvn clean compile -DskipTests exec:java \
-Pdataflow-runner \
-Dexec.mainClass=com.bawi.beam.dataflow.MyPubsubReadWriteJob\$Read \
-Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --topic=projects/${PROJECT}/topics/$USER-${JOB} \
  --jobName=${JOB}-read-$USER \
  --subscription=projects/${PROJECT}/subscriptions/$USER-${JOB}-sub"

*/

    public static class Read {
        public static void main(String[] args) {
            MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
            Pipeline readingPipeline = Pipeline.create(options);

            // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
            readingPipeline.apply(PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(options.getSubscription()))
                    .apply(MapElements.via(new SimpleFunction<PubsubMessage, String>() {
                        @Override
                        public String apply(PubsubMessage msg) {
                            return "body=" + new String(msg.getPayload()) +
                                    ", attributes=" + msg.getAttributeMap() +
                                    ", messageId=" + msg.getMessageId();
                        }
                    }))
                    .apply(MyConsoleIO.write());

            PipelineResult result = readingPipeline.run();
//            result.waitUntilFinish();
        }
    }
}
