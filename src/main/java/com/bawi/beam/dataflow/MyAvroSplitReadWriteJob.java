package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bawi.beam.dataflow.PipelineUtils.*;

public class MyAvroSplitReadWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyAvroSplitReadWriteJob.class);
    private static final String JOB_NAME = "bartek-" + MyAvroSplitReadWriteJob.class.getSimpleName().toLowerCase();
    private static final String OUTPUT_PATH = JOB_NAME + "/output";
    private static final String TEMP_DIRECTORY_PATH = JOB_NAME + "/temp";
    private static final String TEMPLATE_LOCATION = JOB_NAME + "/templates/" + JOB_NAME + "-template";
    private static final String MYDATA = "mydata";
    private static final String SNAPPY_AVRO = ".snappy.avro";
    private static final String CSV_GZ = ".csv.gz";
    private static final String MYDATA_SNAPPY_AVRO = OUTPUT_PATH  + "/" + MYDATA + SNAPPY_AVRO;
    private static final String MYDATA_CSV_GZ = OUTPUT_PATH  + "/" + MYDATA + CSV_GZ;
    private static final String MYDATA2_SNAPPY_AVRO = OUTPUT_PATH  + "2/" + MYDATA + "-";
    private static final String MYDATA2_CSV_GZ = OUTPUT_PATH  + "2/" + MYDATA+ "-";

    public static void main(String[] args) throws IOException, InterruptedException {

        String[] generatorArgs =
                isDataflowRunnerOnClasspath() ? PipelineUtils.updateArgsWithDataflowRunner(args
                , "--jobName=" + JOB_NAME + "-generator"
                ,"--numWorkers=1"
                ,"--workerMachineType=n1-standard-4"
                ,"--avroOutput=gs://" + MYDATA_SNAPPY_AVRO
                ,"--csvOutput=gs://" + MYDATA_CSV_GZ
        ) : updateArgs(args
                ,"--avroOutput=target/" + MYDATA_SNAPPY_AVRO
                ,"--csvOutput=target/" + MYDATA_CSV_GZ
        );

        MyPipelineOptions generatorOptions = PipelineOptionsFactory.fromArgs(generatorArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline generatorPipeline = Pipeline.create(generatorOptions);

        // 300 * 1000 -> 299.8 MB Elements added 300,000 Estimated size 303.27 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 (64MB = 64 * 1024 * 1024) took 551 ms and produced 1 files and 5 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 5 bundles with total serialized response size 11556

        // 100 * 1000 -> 99.9 MB Elements added 100,000 Estimated size 101.09 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 took 599 ms and produced 1 files and 2 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 2 bundles with total serialized response size 4669

        PCollection<MyData> myDataGenerator = generatorPipeline.apply(Create.of(IntStream.rangeClosed(1, 1_000).boxed().collect(Collectors.toList())))
                .apply("MapIntToMyData", MapElements
                        .into(TypeDescriptor.of(MyData.class))
                        .via(i -> new MyData(
                                UUID.randomUUID().toString(),
                                System.currentTimeMillis(),
                                ThreadLocalRandom.current().nextInt(100), RandomStringUtils.secure().nextAlphanumeric((1000)))
                        )
                );

        myDataGenerator
                .apply("WriteToAvro", AvroIO
                        .write(MyData.class)
                        .to(generatorOptions.getAvroOutput())
                        .withCodec(CodecFactory.snappyCodec()) // DEFAULT_CODEC = CodecFactory.snappyCodec() // CodecFactory.nullCodec() for no compression
                        .withoutSharding() // to single file
                );

        // 3a
        myDataGenerator
                .apply("MapMyDataToCSV", MapElements.into(TypeDescriptors.strings()).via(MyData::toCSV))
                .apply("WriteCSVToGZ", TextIO
                        .write()
                        .to(generatorOptions.getCsvOutput().get().replace(".gz", "")) // remove as gzp compression add .gz suffix
                        .withCompression(Compression.GZIP)
                        .withoutSharding()
                );

        generatorPipeline.run().waitUntilFinish();

        boolean useDataflowTemplateInConsumerPipeline = false;
        Set<String> additionalArgs = new HashSet<>();
        additionalArgs.add("--jobName=" + JOB_NAME + "-template-input-in-runtime-t2d8");
        //noinspection ConstantValue
        if (useDataflowTemplateInConsumerPipeline) {
            additionalArgs.add("--templateLocation=gs://" + TEMPLATE_LOCATION);
        } else {
            additionalArgs.addAll(
                    Set.of("--numWorkers=1"
                    , "--maxNumWorkers=1"
                    , "--workerMachineType=t2d-standard-8" // same number of bundles when using t2d-standard-8
                    , "--avroInput=gs://" + MYDATA_SNAPPY_AVRO
                    , "--avroOutput=gs://" + MYDATA2_SNAPPY_AVRO
                    , "--csvInput=gs://" + MYDATA_CSV_GZ
                    , "--csvOutput=gs://" + MYDATA2_CSV_GZ
                    , "--tempDirectory=gs://" + TEMP_DIRECTORY_PATH
                    , "--numShards=1"
//                        ,"--numShards=8"
            ));
        }


            ///////////////////////////
        String[] consumerArgs =
                isDataflowRunnerOnClasspath() ?
                    updateArgsWithDataflowRunner(args, additionalArgs.toArray(new String[0]))
                        :
                    updateArgs(args
                            ,"--avroInput=target/" + MYDATA_SNAPPY_AVRO
                            ,"--avroOutput=target/" + MYDATA2_SNAPPY_AVRO
                            ,"--csvInput=target/" + MYDATA_CSV_GZ
                            ,"--csvOutput=target/" + MYDATA2_CSV_GZ
                            ,"--numShards=1"
                    );

        Schema schema = SchemaBuilder.record("myData").fields().requiredString("uuid").requiredLong("ts").requiredInt("number").optionalString("value").endRecord();

        MyPipelineOptions consumerOptions = PipelineOptionsFactory.fromArgs(consumerArgs).withValidation().as(MyPipelineOptions.class);

        Pipeline consumerPipeline = Pipeline.create(consumerOptions);

        // 1. read
        PCollection<MyData> myDataConsumer = consumerPipeline
                .apply("ReadMyData", AvroIO.read(MyData.class).from(consumerOptions.getAvroInput()));

        // 2a transform mydata and log sum
        myDataConsumer // input needs to be ValueProvider and passed at runtime at job submit (not at template creation)
                .apply("MyDataToNumberFn", ParDo.of(new MyDataToNumberFn()))
                .apply("SumIntsGlobally", Sum.integersGlobally())
                .apply("Log", MapElements.into(TypeDescriptors.voids()).via(i -> {
                    LOGGER.info("Sum MyData={}", i);
                    return null;
                }));

        // 2b. transform MyData to generic records and write generic records of MyData avro schema with FilenamePolicy
        myDataConsumer
                .apply("MyDataToGenRecFn", ParDo.of(new MyDataToGenericRecordFn(schema.toString()))).setCoder(AvroGenericCoder.of(schema)) // required to explicitly set coder for GenericRecord
                .apply("WriteSnappyAvro", AvroIO.writeGenericRecords(schema)
                        .to(new MyFilenamePolicy(consumerOptions.getAvroOutput(), SNAPPY_AVRO))
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(consumerOptions.getTempDirectory(), FileBasedSink::convertToFileResourceIfPossible))
                        .withNumShards(consumerOptions.getNumShards())
                );

        // 3a
        PCollection<String> csvPColl = consumerPipeline
                .apply("ReadCsvGz", TextIO.read().withCompression(Compression.GZIP).from(consumerOptions.getCsvInput()));

        csvPColl
                .apply("MyCSVToNumberFn", ParDo.of(new MyCSVToNumberFn()))
                .apply("SumIntsGlobally", Sum.integersGlobally())
                .apply("Log", MapElements.into(TypeDescriptors.voids()).via(i -> {
                    LOGGER.info("Sum CSV={}", i);
                    return null;
                }));

        csvPColl.
                apply("MyCSVToGenRecFn", ParDo.of(new MyCSVToGenericRecordFn(schema.toString())))
                .setCoder(AvroGenericCoder.of(schema))
                .apply("WriteSnappyAvro", AvroIO.writeGenericRecords(schema)
                        .to(new MyFilenamePolicy(consumerOptions.getCsvOutput(),  SNAPPY_AVRO))
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(consumerOptions.getTempDirectory(), FileBasedSink::convertToFileResourceIfPossible))
                        .withCodec(CodecFactory.snappyCodec())
                        .withNumShards(consumerOptions.getNumShards())
                );


        PipelineResult result = consumerPipeline.run();

        if (!isDataflowRunnerOnClasspath() && result.getClass().getSimpleName().equals("DirectPipelineResult")) {
            LOGGER.info("DirectRunner job submitted, waiting until finish...");
            result.waitUntilFinish();
            LOGGER.info("counters={}", getCounters(result.metrics()));
            LOGGER.info("distributions={}", getDistributions(result.metrics()));
        }
        if (isDataflowRunnerOnClasspath() && result.getClass().getSimpleName().equals("DataflowPipelineJob")) {
            LOGGER.info("Non-templated Dataflow job submitted, waiting until finish...");
            result.waitUntilFinish();
            LOGGER.info("counters={}", getCounters(result.metrics()));
            LOGGER.info("distributions={}", getDistributions(result.metrics()));
        }
        if (isDataflowRunnerOnClasspath() && !result.getClass().getSimpleName().equals("DataflowPipelineJob")) {
            LOGGER.info("Template generation path");
            String cmd = "gcloud dataflow jobs run " + JOB_NAME + "-input-in-runtime-t2d8 "
                    + System.getenv("GCP_GCLOUD_DATAFLOW_RUN_OPTS")
                    + " --num-workers 1"
                    + " --max-workers 1"
                    + " --worker-machine-type t2d-standard-8" // same number of bundles when using t2d-standard-8
                    + " --gcs-location gs://" + JOB_NAME + "/templates/" + JOB_NAME + "-template"
//                    + " --parameters tempDirectory=gs://" + TEMP_DIRECTORY_PATH + ",avroInput=gs://" + MYDATA_SNAPPY_AVRO + ",avroOutput=gs://" + MYDATA2_SNAPPY_AVRO + ",csvInput=gs://" + MYDATA_CSV_GZ + ",csvOutput=gs://" + MYDATA2_CSV_GZ;
//                    + " --parameters tempDirectory=gs://" + TEMP_DIRECTORY_PATH + ",avroInput=gs://" + MYDATA_SNAPPY_AVRO + ",avroOutput=gs://" + MYDATA2_SNAPPY_AVRO;
                    + " --parameters tempDirectory=gs://" + TEMP_DIRECTORY_PATH + ",avroInput=gs://" + MYDATA_SNAPPY_AVRO + ",avroOutput=gs://" + MYDATA2_SNAPPY_AVRO + ",csvInput=gs://" + MYDATA_CSV_GZ + ",csvOutput=gs://" + MYDATA2_CSV_GZ;
            runBashProcessAndWaitForStatus(cmd);
        }
    }

    private static void runBashProcessAndWaitForStatus(String cmd) throws IOException, InterruptedException {
        LOGGER.info("cmd={}", cmd);
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.command("bash", "-c", cmd);
        Process process = processBuilder.start();
        logProcess(process);
        int status = process.waitFor();
        LOGGER.info("status={}", status);
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
        private final Counter bundleCounter = Metrics.counter(MyDataToGenericRecordFn.class.getSimpleName(), "my-bundle-counter");
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
        public static final String MY_DATA_TO_NUMBER_FN_CLASS = MyDataToNumberFn.class.getSimpleName();
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
        return Thread.currentThread().getName() + "_" + Thread.currentThread().threadId();
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

        public String toCSV() {
            return uuid + "," + ts + "," + number + "," + value;
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        ValueProvider<String> getAvroInput();
        void setAvroInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getAvroOutput();
        void setAvroOutput(ValueProvider<String> value);

        ValueProvider<String> getCsvInput();
        void setCsvInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getCsvOutput();
        void setCsvOutput(ValueProvider<String> value);

        ValueProvider<String> getTempDirectory();
        void setTempDirectory(ValueProvider<String> value);

        int getNumShards();
        void setNumShards(int value);
    }

    static class MyFilenamePolicy extends FileBasedSink.FilenamePolicy {
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
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints, filenameSuffix);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            long windowStartMillis = System.currentTimeMillis();
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(outputParentPath.get());
            String outputFilePath = getFilePath(resource, windowStartMillis, shardNumber, numShards, outputFileHints, filenameSuffix);
            return resource.getCurrentDirectory().resolve(outputFilePath, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

        }
    }
}
