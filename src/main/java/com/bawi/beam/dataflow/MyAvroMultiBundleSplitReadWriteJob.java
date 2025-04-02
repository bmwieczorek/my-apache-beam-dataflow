package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
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
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.*;
import java.math.BigInteger;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MyAvroMultiBundleSplitReadWriteJob {
    private static final String JOB_NAME = "bartek-" + MyAvroReadWriteJob.class.getSimpleName().toLowerCase();
    private static final String PROJECT_ID = System.getenv("GCP_PROJECT");
    private static final String BUCKET_NAME = PROJECT_ID + "-" + JOB_NAME;

    public static void main(String[] args) throws IOException, InterruptedException {
        
        
        String[] generatorArgs = PipelineUtils.updateArgsWithDataflowRunner(args
                , "--jobName=bartek-" + JOB_NAME + "-generator"
                ,"--numWorkers=1"
                ,"--workerMachineType=n1-standard-4"
                ,"--output=gs://" + BUCKET_NAME + "/generator/output/mydata.snappy.avro"
        );

//        String[] generatorArgs = PipelineUtils.updateArgs(args
//                ,"--output=target/" + JOB_NAME + "/generator/output/mydata.snappy.avro"//
//        );

        MyPipelineOptions generatorOptions = PipelineOptionsFactory.fromArgs(generatorArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline generatorPipeline = Pipeline.create(generatorOptions);

        // 300 * 1000 -> 299.8 MB Elements added 300,000 Estimated size 303.27 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 (64MB = 64 * 1024 * 1024) took 551 ms and produced 1 files and 5 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 5 bundles with total serialized response size 11556

        // 100 * 1000 -> 99.9 MB Elements added 100,000 Estimated size 101.09 MB
        // Splitting filepattern gs://my-bucket-bartek-myavroreadwritejob/generator/output/* into bundles of size 67108864 took 599 ms and produced 1 files and 2 bundles
        // Splitting source gs://my-bucket-bartek-myavroreadwritejob/generator/output/* produced 2 bundles with total serialized response size 4669

        generatorPipeline.apply(Create.of(IntStream.rangeClosed(1, 100 * 1000).boxed().collect(Collectors.toList())))
                .apply(MapElements.into(TypeDescriptor.of(MyData.class)).via(i ->
                                new MyData(UUID.randomUUID().toString(), System.currentTimeMillis(), new Random().nextInt(100), RandomStringUtils.randomAlphanumeric(1000))))
                .apply(AvroIO.write(MyData.class).to(generatorOptions.getOutput())
//                        .withCodec(CodecFactory.nullCodec()) // DEFAULT_CODEC = CodecFactory.snappyCodec() // CodecFactory.nullCodec() for no compression
                        .withoutSharding()); // to single file
        generatorPipeline.run().waitUntilFinish();


        ///////////////////////////


        String[] readWriteArgs = PipelineUtils.updateArgsWithDataflowRunner(args
                , "--jobName=" + JOB_NAME + "-generator"
                ,"--numWorkers=1"
                ,"--maxNumWorkers=2"
                ,"--workerMachineType=n1-standard-4" // same number of bundles when using t2d-standard-8
                ,"--input=gs://" + BUCKET_NAME + "/generator/output/*"
                ,"--templateLocation=gs://" + BUCKET_NAME + "/templates/" + JOB_NAME + "-template"
        );

//        String[] readWriteArgs = PipelineUtils.updateArgs(args
//                ,"--input=target/" + JOB_NAME + "/generator/output/*.snappy.avro*"
//                ,"--output=target/" + JOB_NAME + "/writing/output/mydata.snappy.avro"
//        );

        Schema schema = SchemaBuilder.record("myData").fields().requiredString("uuid").requiredLong("ts").requiredInt("number").optionalString("value").endRecord();

        MyPipelineOptions readWriteOptions = PipelineOptionsFactory.fromArgs(readWriteArgs).withValidation().as(MyPipelineOptions.class);

        Pipeline readWritePipeline = Pipeline.create(readWriteOptions);
        readWritePipeline
//                .apply(AvroIO.readGenericRecords(schema)
                .apply(AvroIO.read(MyData.class).from(readWriteOptions.getInput())) // input needs to be ValueProvider and passed at runtime at job submit (not at template creation)
                .apply("ParDo MyToGenRecFn", ParDo.of(new MyToGenericRecordFn(schema.toString()))).setCoder(AvroGenericCoder.of(schema)) // required to explicitly set coder for GenericRecord
                .apply(AvroIO.writeGenericRecords(schema).to(readWriteOptions.getOutput()).withoutSharding());
        PipelineResult run = readWritePipeline.run();
        if (run.getClass().getSimpleName().equals("DataflowPipelineJob")) {
            run.waitUntilFinish();
        }

        String cmd = "gcloud dataflow jobs run " + JOB_NAME + "-template "
                + System.getenv("GCP_GCLOUD_DATAFLOW_RUN_OPTS")
                + " --gcs-location gs://" + BUCKET_NAME + "/templates/" + JOB_NAME + "-template"
//                + " --parameters input=gs://" + BUCKET_NAME + "/generator/output/*,output=gs://" + BUCKET_NAME + "/writing/output/*";
                + " --parameters output=gs://" + BUCKET_NAME + "/writing/output/*";

        runBashProcessAndWaitForStatus(cmd);
        
    }

    private static int runBashProcessAndWaitForStatus(String cmd) throws IOException, InterruptedException {
        System.out.println(cmd);
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.command("bash", "-c", cmd);
        Process process = processBuilder.start();
        logProcess(process);
        int status = process.waitFor();
        System.out.println(status);
        return status;
    }

    private static void logProcess(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

    private static class MyToGenericRecordFn extends DoFn<MyData, GenericRecord> {
//    private static class MyToGenericRecordFn extends DoFn<GenericRecord, GenericRecord> {
        private final Distribution elapsedTimeDistribution = Metrics.distribution(MyAvroMultiBundleSplitReadWriteJob.class.getSimpleName(), "my-metrics-elapsed-time-distribution");
        private final Counter writeCounter = Metrics.counter(MyAvroMultiBundleSplitReadWriteJob.class.getSimpleName(), "my-metrics-write-counter");
        private final String schemaString;
        private Schema schema;

        public MyToGenericRecordFn(String schemaString) {
            this.schemaString = schemaString;
        }

        @Setup
        public void init() {
            schema = new Schema.Parser().parse(schemaString);
        }

        @ProcessElement
//        public void process(@Element GenericRecord record, OutputReceiver<GenericRecord> outputReceiver) {
        public void process(@Element MyData myData, OutputReceiver<GenericRecord> outputReceiver) {
            GenericData.Record genericRecord = new GenericData.Record(schema);
            genericRecord.put("uuid", myData.uuid);
            genericRecord.put("ts", myData.ts);
            genericRecord.put("number", myData.number);
//            genericRecord.put("uuid", record.get("uuid"));
//            genericRecord.put("ts", record.get("ts"));
//            genericRecord.put("number", record.get("number"));
            long start = System.currentTimeMillis();
//            genericRecord.put("value", factorial(20000 + myData.number));
            genericRecord.put("value", factorial(1 + myData.number));
//            genericRecord.put("value", factorial(1 + (Integer) record.get("number")));
            elapsedTimeDistribution.update(System.currentTimeMillis() - start);
            outputReceiver.output(genericRecord);
            writeCounter.inc();
        }

        private static String factorial(int limit) {
            BigInteger factorial = BigInteger.valueOf(1);
            for (int i = 1; i <= limit; i++) {
                factorial = factorial.multiply(BigInteger.valueOf(i));
            }
            return factorial.toString();
        }
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
    }
}
