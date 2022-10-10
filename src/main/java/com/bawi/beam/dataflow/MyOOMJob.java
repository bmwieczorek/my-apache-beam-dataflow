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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MyOOMJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyOOMJob.class);

    private static final Schema SCHEMA = SchemaBuilder.record("root").fields()
            .requiredInt("value")
            .optionalString("local_host_address")
            .optionalLong("thread_id")
            .optionalString("thread_name")
            .optionalString("thread_group")
            .optionalString("heap_total")
            .optionalString("heap_free")
            .optionalString("heap_used")
            .optionalString("heap_max")
            .endRecord();

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("bartek_dataset.myoomtest_table")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> value);

        @Validation.Required
        @Default.String("1,2000000")
        ValueProvider<String> getSequenceStartCommaEnd();
        @SuppressWarnings("unused")
        void setSequenceStartCommaEnd(ValueProvider<String> value);
    }

/*

JOB=myoomjob
BUCKET=${GCP_PROJECT}-${GCP_OWNER}-${JOB}
gsutil mb gs://${BUCKET}

mvn clean compile -DskipTests -Pdataflow-runner exec:java \
 -Dexec.mainClass=com.bawi.beam.dataflow.MyOOMJob \
 -Dexec.args="${GCP_JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --workerMachineType=n1-standard-1 \
  --dumpHeapOnOOM \
  --saveHeapDumpsToGcsPath=gs://${BUCKET}/oom \
  --workerLogLevelOverrides='{ \"com.bawi.beam.dataflow.MyOOMJob\": \"DEBUG\" }' \
  --profilingAgentConfiguration='{ \"APICurated\" : true }' \
  --experiments=enable_stackdriver_agent_metrics \
  --labels='{ \"my_job_name\" : \"${JOB}\"}'"

WORKER="$(gcloud compute instances list --filter="name~$JOB" | tail -n 1)"
echo WORKER=$WORKER
WORKER_NAME=$(echo "${WORKER}" | awk '{ print $1 }')
echo WORKER_NAME=$WORKER_NAME
WORKER_ZONE=$(echo "${WORKER}" | awk '{ print $2 }')
echo WORKER_ZONE=$WORKER_ZONE
gcloud compute ssh --project=$PROJECT --zone=$WORKER_ZONE $WORKER_NAME --ssh-flag "-L 5555:127.0.0.1:5555"


java -agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=myoomjob,-cprof_service_version=1.0.0 \
   -cp my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyOOMJob --runner=DataflowRunner --stagingLocation=gs://${BUCKET}/staging --workerMachineType=n1-standard-1

#   --profilingAgentConfiguration='{ \"APICurated\" : true }'"
 */

    public static void main(String[] args) {
        args = DataflowUtils.updateDataflowArgs(args, "--profilingAgentConfiguration={ \"APICurated\" : true }");

/*
        args = DataflowUtils.updateDataflowArgs(args,
                "--profilingAgentConfiguration={\"APICurated\":true}",
                "--dumpHeapOnOOM=true",
                "--diskSizeGb=200",
                "--saveHeapDumpsToGcsPath=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/oom",
                "--maxNumWorkers=1",
                "--workerMachineType=n1-standard-1"
//                "--dataflowServiceOptions=enable_google_cloud_profiler"
        );
*/

        MyPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
//        DataflowProfilingOptions.DataflowProfilingAgentConfiguration profilingConf = new DataflowProfilingOptions.DataflowProfilingAgentConfiguration();
//        profilingConf.put("APICurated", true);
//        pipelineOptions.setProfilingAgentConfiguration(profilingConf);
//        pipelineOptions.setDumpHeapOnOOM(true);
//        pipelineOptions.setSaveHeapDumpsToGcsPath("gs://mybucket-bartek/oom");
//        pipelineOptions.setDataflowServiceOptions(List.of("enable_google_cloud_profiler"));

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        ValueProvider.NestedValueProvider<List<Integer>, String> nestedValueProvider = ValueProvider.NestedValueProvider.of(pipelineOptions.getSequenceStartCommaEnd(), startCommaStop -> {
            int start = Integer.parseInt(startCommaStop.substring(0, startCommaStop.indexOf(",")));
            int end = Integer.parseInt(startCommaStop.substring(startCommaStop.indexOf(",") + 1));
            LOGGER.info("Sequence start={}, end={}", start, end);
            return IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
        });

        ListCoder<Integer> integerListCoder = ListCoder.of(SerializableCoder.of(Integer.class));

        //pipeline.apply(Create.of(IntStream.rangeClosed(10000, 25000).boxed().collect(Collectors.toList())))
        pipeline.apply(Create.ofProvider(nestedValueProvider, integerListCoder))
                .apply(FlatMapElements.into(TypeDescriptors.integers()).via(iter -> iter))

                .apply(MapElements.into(TypeDescriptor.of(GenericRecord.class)).via(i -> {
                    GenericData.Record record = new GenericData.Record(SCHEMA);
                    record.put("value", createBigArrayList(i));
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
                    String total = format(Runtime.getRuntime().totalMemory());
                    record.put("heap_total", total);
                    String free = format(Runtime.getRuntime().freeMemory());
                    record.put("heap_free", free);
                    String used = format(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
                    record.put("heap_used", used);
                    String max = format(Runtime.getRuntime().maxMemory());
                    record.put("heap_max", max);
                    LOGGER.info("i={},localHostAddress={},thread_id={},thread_name={},threadGroup={},heap_total={},heap_free={},heap_used={},heap_max={}",
                            i, localHostAddress, thread.getId(), thread.getName(), thread.getThreadGroup(), total, free, used, max);
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

//    private static String format(long value) {
//        if (value < 1024) return value + "B";
//        if (value < 1024 * 1024) return (value / 1024) + "KB";
//        return (value / 1024 / 1024) + "MB";
//    }

    private static String format(long value) {
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(true);
        return numberFormat.format(value);
    }

    private static int createBigArrayList(int limit) {
        List<byte[]> strings = new ArrayList<>();
        for (int i = 1; i <= limit; i++) {
            strings.add(new byte[2 * 1024]);
        }
        return strings.size();
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
