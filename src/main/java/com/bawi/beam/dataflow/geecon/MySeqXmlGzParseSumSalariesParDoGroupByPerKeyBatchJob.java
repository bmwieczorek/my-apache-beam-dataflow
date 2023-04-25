package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.parser.impl.StringLengthParser;
import com.bawi.parser.impl.SumValuesParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.PipelineUtils.*;
import static com.bawi.beam.dataflow.geecon.XmlPayload.XML_PAYLOAD;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class MySeqXmlGzParseSumSalariesParDoGroupByPerKeyBatchJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySeqXmlGzParseSumSalariesParDoGroupByPerKeyBatchJob.class);
    private static final String JOB_NAME = "bartek-" + MySeqXmlGzParseSumSalariesParDoGroupByPerKeyBatchJob.class.getSimpleName().toLowerCase();

    public static void main(String[] args) throws IOException {
        String[] updatedArgs =
                isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--50m-elements--3x-t2dst2"
                                , "--numWorkers=3"
                                , "--maxNumWorkers=3"
                                , "--workerMachineType=t2d-standard-2"
                                , "--sequenceLimit=50000000"
                                , "--experiments=enable_stackdriver_agent_metrics"
                        ) :
                        PipelineUtils.updateArgs(args
                                , "--sequenceLimit=10"
                        );

        byte[] SINGLE_PAYLOAD_GZ = gzip(XML_PAYLOAD);

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply(GenerateSequence.from(0).to(opts.getSequenceLimit()))
                .apply("GenSingleXmlGzip", MapElements.into(kvs(strings(), of(byte[].class)))
                        .via(n -> KV.of(distribute(n), SINGLE_PAYLOAD_GZ)))

                .apply("SumSalariesParDo", ParDo.of(new GunzipParseXml()))

                .apply(GroupByKey.create())
                .apply(Combine.groupedValues(Sum.ofLongs()))
                // or
//                .apply(Combine.perKey(Sum.ofLongs()))

                .apply("LogSumSalariesPK", MapElements.into(voids()).via(kv -> {
                    LOGGER.info(kv.toString());
                    return null;
                }))
        ;

        PipelineResult result = pipeline.run();
        if (result.getClass().getSimpleName().equals("DataflowPipelineJob") || result.getClass().getSimpleName().equals("DirectPipelineResult")) {
            result.waitUntilFinish();
            LOGGER.info("counters={}", getCounters(result.metrics()));
            LOGGER.info("distributions={}", getDistributions(result.metrics()));
        }
    }

    private static String distribute(Long n) {
        return List.of("US", "US", "PL", "US", "MT", "US", "US", "PL", "US", "US").get((int) (n % 10));
    }

    static class GunzipParseXml extends DoFn<KV<String, byte[]>, KV<String, Long>> {
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<VtdXmlParser.FieldXpathMappingEntry> mapping = List.of(
                    new VtdXmlParser.FieldXpathMappingEntry("college_first_staff_dep_name", "staff[1]/@dep_name", String.class),
                    new VtdXmlParser.FieldXpathMappingEntry("college_first_staff_dep_name_length", "staff[1]/@dep_name", StringLengthParser.class),
                    new VtdXmlParser.FieldXpathMappingEntry("staff_basic_salary_sum", "staff/salary/basic", SumValuesParser.class),
                    new VtdXmlParser.FieldXpathMappingEntry("staff_id_attr_sum", "staff/@id", SumValuesParser.class),
                    new VtdXmlParser.FieldXpathMappingEntry("college_id", "@id", Integer.class)
            );
            vtdXmlParser = new VtdXmlParser(mapping);
        }

        @ProcessElement
        public void process(@Element KV<String, byte[]> elem, OutputReceiver<KV<String, Long>> receiver) {
            Metrics.counter("keyToHostname", elem.getKey() + "_" + hostname()).inc();

            // do not log to avoid: Throttling logger worker. It used up its 30s quota for logs in only ... sec
            // LOGGER.info("keyToHostname {}", element.getKey() + "_" + getHostname()

            long start = currentTimeMillis();
            String xml = new String(gunzip(elem.getValue()));
            Map<String, Object> xmlAsMap = vtdXmlParser.parseXml(xml);
            long salariesSumInXml = (int) xmlAsMap.get("staff_basic_salary_sum");
            Metrics.distribution(GunzipParseXml.class, "elapsedTime").update(currentTimeMillis() - start);
            receiver.output(KV.of(elem.getKey(), salariesSumInXml));
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceLimit();

        @SuppressWarnings("unused")
        void setSequenceLimit(long value);
    }
}
