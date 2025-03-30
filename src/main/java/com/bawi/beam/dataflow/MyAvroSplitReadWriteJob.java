package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.PipelineUtils.getCounters;
import static com.bawi.beam.dataflow.PipelineUtils.getDistributions;

public class MyAvroSplitReadWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyAvroSplitReadWriteJob.class);
    private static final String JOB_NAME = "bartek-" + MyAvroSplitReadWriteJob.class.getSimpleName().toLowerCase();
    private static final String PROJECT_ID = System.getenv("GCP_PROJECT");
    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME;

    public static void main(String[] args) throws IOException, InterruptedException {

/*
//        String[] generatorArgs = PipelineUtils.updateArgsWithDataflowRunner(args
//                , "--jobName=bartek-" + JOB_NAME + "-generator"
//                ,"--numWorkers=1"
//                ,"--workerMachineType=n1-standard-4"
//                ,"--output=gs://" + BUCKET_NAME + "/generator/output/mydata.snappy.avro"
//        );

        String[] generatorArgs = PipelineUtils.updateArgs(args
                ,"--output=target/" + JOB_NAME + "/generator/output/mydata.snappy.avro"//
        );

        MyPipelineOptions generatorOptions = PipelineOptionsFactory.fromArgs(generatorArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline generatorPipeline = Pipeline.create(generatorOptions);

        // 300 * 1000 -> 299.8 MB Elements added 300,000 Estimated size 303.27 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 (64MB = 64 * 1024 * 1024) took 551 ms and produced 1 files and 5 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 5 bundles with total serialized response size 11556

        // 100 * 1000 -> 99.9 MB Elements added 100,000 Estimated size 101.09 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 took 599 ms and produced 1 files and 2 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 2 bundles with total serialized response size 4669

        generatorPipeline.apply(Create.of(IntStream.rangeClosed(1, 1 * 1000).boxed().collect(Collectors.toList())))
                .apply(MapElements.into(TypeDescriptor.of(MyData.class)).via(i ->
                                new MyData(UUID.randomUUID().toString(), System.currentTimeMillis(), new Random().nextInt(100), RandomStringUtils.randomAlphanumeric(1000))))
                .apply(AvroIO.write(MyData.class).to(generatorOptions.getOutput())
//                        .withCodec(CodecFactory.nullCodec()) // DEFAULT_CODEC = CodecFactory.snappyCodec() // CodecFactory.nullCodec() for no compression
                        .withoutSharding()); // to single file
        generatorPipeline.run().waitUntilFinish();

*/


        ///////////////////////////
        String[] readWriteArgs =
                PipelineUtils.isDataflowRunnerOnClasspath() ?
                    PipelineUtils.updateArgsWithDataflowRunner(args
                        , "--jobName=" + JOB_NAME + "-template-input-in-runtime-t2d8"
                        ,"--numWorkers=1"
                        ,"--maxNumWorkers=1"
                        ,"--workerMachineType=t2d-standard-8" // same number of bundles when using t2d-standard-8
                        ,"--input=gs://" + BUCKET_NAME + "/generator/output/*"
//                        ,"--output=gs://" + BUCKET_NAME + "/writing/output/mydata.snappy.avro"
                        ,"--templateLocation=gs://" + BUCKET_NAME + "/templates/" + JOB_NAME + "-template"
//                        ,"--numShards=8"
                    ) :
                    PipelineUtils.updateArgs(args
                            ,"--input=target/bartek-myseqgentogcswritejob/generator/output/mydata.csv.gz"
//                            ,"--input=target/bartek-myseqgentogcswritejob/generator/output/mydata.snappy.avro"
            //                ,"--output=target/" + JOB_NAME + "/writing/output/mydata.snappy.avro"
//                            ,"--output=target/" + JOB_NAME + "/writing/output/"
//                            ,"--numShards=1"
                    );

        Schema schema = SchemaBuilder.record("myData").fields().requiredString("uuid").requiredLong("ts").requiredInt("number").optionalString("value").endRecord();

        MyPipelineOptions readWriteOptions = PipelineOptionsFactory.fromArgs(readWriteArgs).withValidation().as(MyPipelineOptions.class);

        Pipeline readWritePipeline = Pipeline.create(readWriteOptions);
        readWritePipeline
//                .apply(AvroIO.read(MyData.class).from(readWriteOptions.getInput())) // input needs to be ValueProvider and passed at runtime at job submit (not at template creation)
//                .apply("MyDataToNumberFn", ParDo.of(new MyDataToNumberFn()))

                .apply(TextIO.read().withCompression(Compression.GZIP).from(readWriteOptions.getInput()))
                .apply("MyCSVToNumberFn", ParDo.of(new MyCSVToNumberFn()))

                .apply(Sum.integersGlobally())
                .apply(MapElements.into(TypeDescriptors.voids()).via(i -> {
                    LOGGER.info("Sum={}", i);
                    return null;
                }));

//                .apply("MyDataToGenRecFn", ParDo.of(new MyDataToGenericRecordFn(schema.toString()))).setCoder(AvroGenericCoder.of(schema)) // required to explicitly set coder for GenericRecord
//                .apply(AvroIO.writeGenericRecords(schema).to(readWriteOptions.getOutput())
//                        .to(new MyFilenamePolicy(readWriteOptions.getOutput(), ".snappy.avro"))
//                        .withNumShards(readWriteOptions.getNumShards())
//                );

//                .apply(AvroIO.writeGenericRecords(schema).to(readWriteOptions.getOutput())
//                        .to(new MyFilenamePolicy(readWriteOptions.getOutput(), ".snappy.avro"))
//                        .withNumShards(readWriteOptions.getNumShards())
//                );

        PipelineResult result = readWritePipeline.run();
        if (result.getClass().getSimpleName().equals("DataflowPipelineJob") || result.getClass().getSimpleName().equals("DirectPipelineResult")) {
            result.waitUntilFinish();
            LOGGER.info("counters={}", getCounters(result.metrics()));
            LOGGER.info("distributions={}", getDistributions(result.metrics()));
        }

        if (PipelineUtils.isDataflowRunnerOnClasspath()) {
            String cmd = "gcloud dataflow jobs run " + JOB_NAME + "-input-in-runtime-t2d8 "
                    + System.getenv("GCP_GCLOUD_DATAFLOW_RUN_OPTS")
                    + " --gcs-location gs://" + BUCKET_NAME + "/templates/" + JOB_NAME + "-template";
//                + " --parameters input=gs://" + BUCKET_NAME + "/generator/output/*,output=gs://" + BUCKET_NAME + "/writing/output/mydata.snappy.avro";
            runBashProcessAndWaitForStatus(cmd);
        }
        
    }

    private static int runBashProcessAndWaitForStatus(String cmd) throws IOException, InterruptedException {
        LOGGER.info("cmd={}", cmd);
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.command("bash", "-c", cmd);
        Process process = processBuilder.start();
        logProcess(process);
        int status = process.waitFor();
        LOGGER.info("status={}", status);
        return status;
    }

    private static void logProcess(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                LOGGER.info(line);
            }
        }
    }

    private static class MyDataToGenericRecordFn extends DoFn<MyData, GenericRecord> {
        private final Distribution elapsedTimeDistribution = Metrics.distribution(MyDataToGenericRecordFn.class.getSimpleName(), "my-elapsed-time-distribution");
        private final Counter writeCounter = Metrics.counter(MyDataToGenericRecordFn.class.getSimpleName(), "my-write-counter");
        private final Counter bundleCounter = Metrics.counter(MyDataToGenericRecordFn.class.getSimpleName(), "my-write-counter");
        private final String schemaString;
        private Schema schema;

        public MyDataToGenericRecordFn(String schemaString) {
            this.schemaString = schemaString;
        }

        @Setup
        public void init() {
            schema = new Schema.Parser().parse(schemaString);
        }

        @StartBundle
        public void startBundle() {
            bundleCounter.inc();
            Metrics.counter(MyAvroSplitReadWriteJob.class.getSimpleName(), "my-bundle-thread-distribution__" + getThreadInfo()).inc();
        }

        @ProcessElement
        public void process(@Element MyData myData, OutputReceiver<GenericRecord> outputReceiver) {
            GenericData.Record genericRecord = new GenericData.Record(schema);
            genericRecord.put("uuid", myData.uuid);
            genericRecord.put("ts", myData.ts);
            genericRecord.put("number", myData.number);
            long start = System.currentTimeMillis();
            genericRecord.put("value", factorial(10000 + myData.number));
            elapsedTimeDistribution.update(System.currentTimeMillis() - start);
            Metrics.counter(MyDataToGenericRecordFn.class.getSimpleName(), "my-thread-distribution__" + getThreadInfo()).inc();
            outputReceiver.output(genericRecord);
            writeCounter.inc();
        }
    }

    private static class MyCSVToGenericRecordFn extends DoFn<String, GenericRecord> {
        private final Distribution elapsedTimeDistribution = Metrics.distribution(MyCSVToGenericRecordFn.class.getSimpleName(), "my-elapsed-time-distribution");
        private final Counter writeCounter = Metrics.counter(MyCSVToGenericRecordFn.class.getSimpleName(), "my-write-counter");
        private final Counter bundleCounter = Metrics.counter(MyCSVToGenericRecordFn.class.getSimpleName(), "my-write-counter");
        private final String schemaString;
        private Schema schema;

        public MyCSVToGenericRecordFn(String schemaString) {
            this.schemaString = schemaString;
        }

        @Setup
        public void init() {
            schema = new Schema.Parser().parse(schemaString);
        }

        @StartBundle
        public void startBundle() {
            bundleCounter.inc();
            Metrics.counter(MyAvroSplitReadWriteJob.class.getSimpleName(), "my-bundle-thread-distribution__" + getThreadInfo()).inc();
        }

        @ProcessElement
        public void process(@Element String csv, OutputReceiver<GenericRecord> outputReceiver) {
            GenericData.Record genericRecord = new GenericData.Record(schema);
            String[] split = csv.split(",");
            genericRecord.put("uuid", split[0]);
            genericRecord.put("ts", Long.parseLong(split[1]));
            int number = Integer.parseInt(split[2]);
            genericRecord.put("number", number);
            long start = System.currentTimeMillis();
            genericRecord.put("value", factorial(10000 + number));
            elapsedTimeDistribution.update(System.currentTimeMillis() - start);
            Metrics.counter(MyCSVToGenericRecordFn.class.getSimpleName(), "my-thread-distribution__" + getThreadInfo()).inc();
            outputReceiver.output(genericRecord);
            writeCounter.inc();
        }


    }

    private static class MyDataToNumberFn extends DoFn<MyData, Integer> {
        public static final String MY_DATA_TO_NUMBER_FN_CLASS = MyDataToGenericRecordFn.class.getSimpleName();
        private final Distribution elapsedTimeDistribution = Metrics.distribution(MY_DATA_TO_NUMBER_FN_CLASS, "my-elapsed-time-distribution");
        private final Counter writeCounter = Metrics.counter(MY_DATA_TO_NUMBER_FN_CLASS, "my-write-counter");
        private final Counter bundleCounter = Metrics.counter(MY_DATA_TO_NUMBER_FN_CLASS, "my-write-counter");

        @StartBundle
        public void startBundle() {
            bundleCounter.inc();
            Metrics.counter(MyAvroSplitReadWriteJob.class.getSimpleName(), "my-bundle-thread-distribution__" + getThreadInfo()).inc();
        }

        @ProcessElement
        public void process(@Element MyData myData, OutputReceiver<Integer> outputReceiver) {
            long start = System.currentTimeMillis();
            String factorial = factorial(10000 + myData.number);
            elapsedTimeDistribution.update(System.currentTimeMillis() - start);
            Metrics.counter(MY_DATA_TO_NUMBER_FN_CLASS, "my-thread-distribution__" + getThreadInfo()).inc();
            outputReceiver.output(factorial.length());
            writeCounter.inc();
        }
    }


    private static class MyCSVToNumberFn extends DoFn<String, Integer> {
        private static final String MY_CSV_TO_NUMBER_FN_CLASS = MyCSVToNumberFn.class.getSimpleName();
        private final Distribution elapsedTimeDistribution = Metrics.distribution(MY_CSV_TO_NUMBER_FN_CLASS, "my-elapsed-time-distribution");
        private final Counter writeCounter = Metrics.counter(MY_CSV_TO_NUMBER_FN_CLASS, "my-write-counter");
        private final Counter bundleCounter = Metrics.counter(MY_CSV_TO_NUMBER_FN_CLASS, "my-write-counter");

        @StartBundle
        public void startBundle() {
            bundleCounter.inc();
            Metrics.counter(MY_CSV_TO_NUMBER_FN_CLASS, "my-bundle-thread-distribution__" + getThreadInfo()).inc();
        }

        @ProcessElement
        public void process(@Element String csv, OutputReceiver<Integer> outputReceiver) {
            String[] split = csv.split(",");
            int number = Integer.parseInt(split[2]);
            long start = System.currentTimeMillis();
            String factorial = factorial(10000 + number);
            elapsedTimeDistribution.update(System.currentTimeMillis() - start);
            Metrics.counter(MY_CSV_TO_NUMBER_FN_CLASS, "my-thread-distribution__" + getThreadInfo()).inc();
            outputReceiver.output(factorial.length());
            writeCounter.inc();
        }


    }

    private static String factorial(int limit) {
        BigInteger factorial = BigInteger.valueOf(1);
        for (int i = 1; i <= limit; i++) {
            factorial = factorial.multiply(BigInteger.valueOf(i));
        }
        return factorial.toString();
    }


    private static String getThreadInfo() {
        return Thread.currentThread().getName() + "_" + Thread.currentThread().getId();
    }

    @DefaultSchema(JavaFieldSchema.class)
    static class MyData implements Serializable {
        public String uuid;
        public Long ts;
        public Integer number;
        @Nullable public String value;

        MyData() { }

        MyData(String uuid, Long ts, Integer number, String value) {
            this.uuid = uuid;
            this.ts = ts;
            this.number = number;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyData myData = (MyData) o;
            return Objects.equals(uuid, myData.uuid) &&
                    Objects.equals(ts, myData.ts) &&
                    Objects.equals(number, myData.number) &&
                    Objects.equals(value, myData.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, ts, number, value);
        }

        @Override
        public String toString() {
            return "MyData{uuid='" + uuid + ", ts=" + ts + ", number=" + number + ", value=" + value + '}';
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);

        int getNumShards();
        void setNumShards(int value);
    }

    static class MyFilenamePolicy extends FileBasedSink.FilenamePolicy {
        private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");

        private final ValueProvider<String> outputParentPath;
        private final String filenameSuffix;

        MyFilenamePolicy(ValueProvider<String> outputParentPath, String filenameSuffix) {
            this.outputParentPath = outputParentPath;
            this.filenameSuffix = filenameSuffix;
        }

        @Override
        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            long windowStartMillis = ((IntervalWindow) window).start().getMillis();
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            long windowStartMillis = System.currentTimeMillis();
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

        }

        private String getFilePath(ResourceId resource, long timestampMillis, int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            String parentDirectoryPath = resource.isDirectory() ? resource.toString() : resource.getFilename();
            String suggestedFilenameSuffix = outputFileHints.getSuggestedFilenameSuffix();
            String suffix = suggestedFilenameSuffix == null || suggestedFilenameSuffix.isEmpty() ? filenameSuffix : suggestedFilenameSuffix;
            String filename = String.format("%s--%s-of-%s%s", FORMATTER.print(timestampMillis), shardNumber, numShards, suffix);
            String randomFilePrefix = DigestUtils.md5Hex(UUID.randomUUID() + filename + timestampMillis).substring(0, 6);
            String outputFilePath = String.format("%s%s--%s", parentDirectoryPath, randomFilePrefix, filename);
            LOGGER.info("Writing file to {}", outputFilePath);
            return outputFilePath;
        }
    }
}
