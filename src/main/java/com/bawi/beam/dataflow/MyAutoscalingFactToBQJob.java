package com.bawi.beam.dataflow;

import com.bawi.beam.dataflow.schema.AvroToBigQuerySchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MyAutoscalingFactToBQJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyAutoscalingFactToBQJob.class);

    private static Schema SCHEMA = SchemaBuilder.record("root").fields()
                .requiredString("value")
                .optionalString("local_host_address")
                .optionalLong("thread_id")
                .optionalString("thread_name")
                .optionalString("thread_group")
            .endRecord();

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("bartek_dataset.myautoscalingtest_table")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> value);

        @Validation.Required
        @Default.String("1000,10000")
        ValueProvider<String> getSequenceStartCommaEnd();
        void setSequenceStartCommaEnd(ValueProvider<String> value);
    }

/*

#PROJECT=$(gcloud config get-value project)
JOB=myautoscalingfacttobqjob
OWNER=bartek
BUCKET=${PROJECT}-$OWNER-${JOB}
gsutil mb gs://${BUCKET}

mvn clean compile -DskipTests -Pdataflow-runner exec:java \
 -Dexec.mainClass=com.bawi.beam.dataflow.MyAutoscalingFactToBQJob \
 -Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --workerMachineType=n1-standard-1"

 */

    public static void main(String[] args) {
        MyPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        ValueProvider.NestedValueProvider<List<Integer>, String> nestedValueProvider = ValueProvider.NestedValueProvider.of(pipelineOptions.getSequenceStartCommaEnd(), startCommaStop -> {
            int start = Integer.parseInt(startCommaStop.substring(0, startCommaStop.indexOf(",")));
            int end = Integer.parseInt(startCommaStop.substring(startCommaStop.indexOf(",") + 1));
            LOGGER.info("Sequence start={}, end={}", start, end);
            return IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
        });

        ListCoder<Integer> integerListCoder = ListCoder.of(SerializableCoder.of(Integer.class));

        //pipeline.apply(Create.of(IntStream.rangeClosed(10000, 25000).boxed().collect(Collectors.toList())))
        //LOGGER.info("element {}, schema {}", r.getElement(), r.getSchema());
        pipeline.apply(Create.ofProvider(nestedValueProvider, integerListCoder))
                .apply(FlatMapElements.into(TypeDescriptors.integers()).via(iter -> iter))

                .apply(MapElements.into(TypeDescriptor.of(GenericRecord.class)).via(i -> {
                    GenericData.Record record = new GenericData.Record(SCHEMA);
                    String factorial = factorial(i);
                    record.put("value", factorial.substring(0, Math.min(factorial.length(), 100)));
                    InetAddress localHostAddress = getLocalHostAddress();
                    if (localHostAddress != null) {
                        record.put("local_host_address", localHostAddress.toString());
                    }
                    Thread thread = Thread.currentThread();
                    record.put("thread_id", thread.getId());
                    record.put("thread_name", thread.getName());
                    ThreadGroup threadGroup = thread.getThreadGroup();
                    if (threadGroup != null) {
                        record.put("thread_group", threadGroup.getName());
                    }
                    LOGGER.info("i={},localHostAddress={},thread_id={},thread_name={},threadGroup={}", i, localHostAddress, thread.getId(), thread.getName(), thread.getThreadGroup());
                    return record;
                })).setCoder(AvroGenericCoder.of(SCHEMA))

                // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
                .apply(BigQueryIO.<GenericRecord>write()
                        .withAvroFormatFunction(AvroWriteRequest::getElement)
                        .withAvroSchemaFactory(qTableSchema -> SCHEMA)
                        .to(pipelineOptions.getTable())
                        .useAvroLogicalTypes()
                        .withSchema(AvroToBigQuerySchemaConverter.convert(SCHEMA))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

//                .apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(pipelineOptions.getOutput()));

        pipeline.run().waitUntilFinish();
    }

    private static String factorial(int limit) {
        BigInteger factorial = BigInteger.valueOf(1);
        for (int i = 1; i <= limit; i++) {
            factorial = factorial.multiply(BigInteger.valueOf(i));
        }
        return factorial.toString();
    }

    private static InetAddress getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }
}
