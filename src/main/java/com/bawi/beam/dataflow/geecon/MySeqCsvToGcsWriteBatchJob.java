package com.bawi.beam.dataflow.geecon;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

import static com.bawi.beam.dataflow.PipelineUtils.*;
import static org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs;
import static org.apache.beam.sdk.options.ValueProvider.NestedValueProvider.of;
import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class MySeqCsvToGcsWriteBatchJob {

    private static final String JOB_NAME = "bartek-" + MySeqCsvToGcsWriteBatchJob.class.getSimpleName().toLowerCase() + "--random-payload--16shards--e2st6";
//    private static final String JOB_NAME = "bartek-" + MySeqToGcsWriteBatchJob.class.getSimpleName().toLowerCase() + "--static-payload--16shards--e2st6";
    private static final String PROJECT_ID = System.getenv("GCP_PROJECT");
    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME; // single region us-central1
//    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME + "-mr"; // multi-region us

    private static final String STATIC_PAYLOAD = "462228a7-207d-46f2-88a4-092b26e6aef5,1681651204821,59,yULwqoD6OHhIUMMvMf6OVq2ktSUBgHLnwxn1tiLULzaaKuWEmCY73P71MR2FWGbCfNBIEIqgkcRzFNJToq83a0kYbfedkJLFCf4mQWsTdFZWlSplns94cDGCI8Z5XhyCHidnXOBcrpulgO3LfhdO9qU5VPjWiDB3LenF1mJxMRIfycyHZK7keU4ek3s2PYLhHPwuK5ZD12Ss5kmW4gJtGeY2RBVYtSfsGBe39KOiRXlZAf2r5dWhNg39rA5LGdM4z7GNG36qSDJEY4krQsHFQvhVfM3739ZcKGkm1dCeS12S2QDoyRXsww0APPlXrYUadqU5m0eHkRb47YafpzUWZi03YyY8GAjQ0Exda8XrRxvlP31I0E7pncylGijMEGwYalSgo1iVqiI9laGxiEYhjNWRzEOB1yCHoB4Rtt6zqlZY4y1Vnhcf0anSOU2kZipezHJ0jMxk4sFTbY6ZSXe4zPjy8Faz10Dm3xV3hGCPbjp2pwOKStyvjPEJIbCXLm4JI8TEKzACCKxExma6lLzbPGUrMxjJjHYFQKCrq7ojngIXqtm3vpxOEUxOQwKKuDHABqaWnG1TE4B1e7oKHl0GtM9snEoaeWwJ6IfZcLXDECqkY5gC9OMx0cfYaCuTeSnn4eCZopCeS9zb48yOBZt3UCxBeHA7D43YnPOVrtJX8kgzwXfan5RPD0ODqAmbb8pmrh3T6NzFLfFK7ejYPEd2Yzysa9q3FtlciHeLKhNhsswzumpm41qB8LDmAzaFgbm2zziEJxm3WyjbgKZI3kQJRkrrLe8DUC0RzxhH0PJnqheJM8VRoY1pLdFQAprDAdoSmp9xfuWy3vdScleKa7a8Zd1JSQf7X8X70D7t4aP2qSgxpdmRuAyYc6JDLk3iC0u9rZo46LG9VZ93twGTSfKHUuYiVQMCj50TsnpR0k4cUE9isw1pwdUqv9wYxgq9pw4ElCfTM29qSvlZtUlSaaWXzVIAkg5qwUyJNIVpmpNZ";

    public static void main(String[] args) {

        String[] updatedArgs =
                isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--random-payload-16shards-e2st8"
                                , "--numWorkers=1"
                                , "--maxNumWorkers=1"
                                , "--workerMachineType=e2-standard-16"
                                , "--outputDir=gs://" + BUCKET_NAME + "/output/"
                                , "--tempDir=gs://" + BUCKET_NAME + "/temp/"
                                , "--sequenceLimit=8000000"
                                , "--numShards=128"
                        ) :
                        updateArgs(args
                                , "--outputDir=target/" + JOB_NAME + "/output/"
                                , "--tempDir=target/" + JOB_NAME + "/temp/"
                                , "--sequenceLimit=10"
                                , "--numShards=4"
                        );

        MyPipelineOptions opts = fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

//        Pipeline pipeline = Pipeline.create(fromArgs(merge(args,"--sequenceLimit=8000000",
//                "--numShards=16","--runner=DataflowRunner","--workerMachineType=e2-standard-16")).as(MyPipelineOptions.class));

        pipeline.apply(GenerateSequence.from(0).to(opts.getSequenceLimit()).withTimestampFn(i -> Instant.now()))

                .apply(MapElements.into(of(String.class)).via(n ->
//                        n + "," + randomUUID() + "," + currentTimeMillis() + "," + new Random().nextInt(100) + "," + randomAlphanumeric(1000)))
                        n + "," + STATIC_PAYLOAD))

                .apply(TextIO.write()
//                        .withWindowedWrites() // single vs multi region to generate small files
                        .to(new CustomFilenamePolicy(opts.getOutputDir(), ".csv"))
                        .withTempDirectory(of(opts.getTempDir(), convertToFileResource()))
                        .withNumShards(opts.getNumShards()));

        pipeline.run().waitUntilFinish();
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutputDir();
        @SuppressWarnings("unused")
        void setOutputDir(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getTempDir();
        @SuppressWarnings("unused")
        void setTempDir(ValueProvider<String> value);

        @Validation.Required
        long getSequenceLimit();
        void setSequenceLimit(long value);

        @Validation.Required
        int getNumShards();
        void setNumShards(int value);
    }

    private static SerializableFunction<String, ResourceId> convertToFileResource() {
        return FileBasedSink::convertToFileResourceIfPossible;
    }
}
