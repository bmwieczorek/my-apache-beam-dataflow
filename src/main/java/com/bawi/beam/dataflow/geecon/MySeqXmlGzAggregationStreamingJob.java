package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.io.GzipUtils;
import com.bawi.parser.impl.StringLengthParser;
import com.bawi.parser.impl.SumValuesParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.PipelineUtils.isDataflowRunnerOnClasspath;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.joda.time.Duration.standardSeconds;
import static org.joda.time.Instant.now;

public class MySeqXmlGzAggregationStreamingJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySeqXmlGzAggregationStreamingJob.class);
    private static final String JOB_NAME = "bartek-" + MySeqXmlGzAggregationStreamingJob.class.getSimpleName().toLowerCase();

    public static void main(String[] args) throws IOException {
        String[] updatedArgs =
                isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--30000-elements-per-sec--3x-t2dst2"
//                                , "--jobName=" + JOB_NAME + "--random-key-suffix--30000-elements-per-sec--3x-t2dst2"
                                , "--numWorkers=3"
                                , "--maxNumWorkers=3"
                                , "--workerMachineType=t2d-standard-2"
                                , "--sequenceRate=30000"
                                , "--experiments=enable_stackdriver_agent_metrics"
                        ) :
                        PipelineUtils.updateArgs(args
                                , "--sequenceRate=1"
                        );

        byte[] SINGLE_PAYLOAD_GZ = gzip(PAYLOAD);

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply(GenerateSequence.from(0)
                        .withRate(opts.getSequenceRate(), standardSeconds(1)).withTimestampFn(i -> now()))
                .apply("GenSingleXmlGzip", MapElements.into(kvs(strings(), of(byte[].class)))
                        .via(n -> KV.of(distribute(n), SINGLE_PAYLOAD_GZ)))
                .apply("Window10sec", Window.into(FixedWindows.of(standardSeconds(10))))

//                .apply("AddKeySuffix", ParDo.of(new DoFn<KV<String, byte[]>, KV<String, byte[]>>() {
//                    private final Random random = new Random();
//
//                    @ProcessElement
//                    public void process(ProcessContext ctx) {
//                        String newKey = ctx.element().getKey() + "/" + random.nextInt(20);
//                        ctx.output(KV.of(newKey, ctx.element().getValue()));
//                    }
//                }))

                .apply(GroupByKey.create())
                .apply("GunzipParseXmls", ParDo.of(new GunzipParseGroupedXmls()))

//                .apply("RemoveKeySuffix", MapElements.into(kvs(strings(), longs())).via(kv -> {
//                    String originalKey = kv.getKey().substring(0, kv.getKey().indexOf("/"));
//                    return KV.of(originalKey, kv.getValue());
//                }))

                .apply("CombinePerKey(Sum)", Sum.longsPerKey())

//                .apply("LogSumAllSalariesPW", MapElements.into(voids()).via(kv -> {
//                    LOGGER.info(kv.toString());
//                    return null;
//                }))
                ;
        pipeline.run();

    }

    private static String distribute(Long n) {
        return List.of("US", "US", "PL", "US", "MT", "US", "US", "PL", "US", "US").get((int) (n % 10));
    }

    static class GunzipParseGroupedXmls extends DoFn<KV<String, Iterable<byte[]>>, KV<String, Long>> {
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
        public void process(@Element KV<String, Iterable<byte[]>> elem, OutputReceiver<KV<String, Long>> receiver) {
            long salariesSumInXmlsGroup = stream(elem.getValue().spliterator(), false)
                .map(bytes -> new String(gunzip(bytes)))

                .peek(xml -> Metrics.counter("keyToHostname", elem.getKey() + "_" + hostname()).inc())

                // do not log to avoid: Throttling logger worker. It used up its 30s quota for logs in only ... sec
                //.peek(xml -> LOGGER.info("keyToHostname {}", element.getKey() + "_" + getHostname()))

                .map(xml -> {
                    long start = currentTimeMillis();
                    Map<String, Object> xmlAsMap = vtdXmlParser.parseXml(xml);
                    Metrics.distribution(VtdXmlParser.class, "elapsedTime").update(currentTimeMillis() - start);
                    return xmlAsMap;
                })

                .map(xmlAsMap -> (long) xmlAsMap.get("staff_basic_salary_sum"))
                .reduce(0L, Long::sum);
//                    .count();
            receiver.output(KV.of(elem.getKey(), salariesSumInXmlsGroup));
        }
}

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceRate();
        void setSequenceRate(long value);
    }

    private static final String PAYLOAD = "" +
            "<college id=\"1\">\n" +
            "    <staff id=\"101\" dep_name=\"Admin\">\n" +
            "        <employee id=\"101-01\" name=\"ashish\"/>\n" +
            "        <employee id=\"101-02\" name=\"amit\"/>\n" +
            "        <employee id=\"101-03\" name=\"nupur\"/>\n" +
            "        <salary id=\"101-sal\">\n" +
            "            <basic>20000</basic>\n" +
            "            <special-allowance>50000</special-allowance>\n" +
            "            <medical>10000</medical>\n" +
            "            <provident-fund>10000</provident-fund>\n" +
            "        </salary>\n" +
            "    </staff>\n" +
            "    <staff id=\"102\" dep_name=\"HR\">\n" +
            "        <employee id=\"102-01\" name=\"shikhar\"/>\n" +
            "        <employee id=\"102-02\" name=\"sanjay\"/>\n" +
            "        <employee id=\"102-03\" name=\"ani\"/>\n" +
            "        <salary id=\"102-sal\">\n" +
            "            <basic>25000</basic>\n" +
            "            <special-allowance>60000</special-allowance>\n" +
            "            <medical>10000</medical>\n" +
            "            <provident-fund>12000</provident-fund>\n" +
            "        </salary>\n" +
            "    </staff>\n" +
            "    <staff id=\"103\" dep_name=\"IT\">\n" +
            "        <employee id=\"103-01\" name=\"suman\"/>\n" +
            "        <employee id=\"103-02\" name=\"salil\"/>\n" +
            "        <employee id=\"103-03\" name=\"amar\"/>\n" +
            "        <salary id=\"103-sal\">\n" +
            "            <basic>35000</basic>\n" +
            "            <special-allowance>70000</special-allowance>\n" +
            "            <medical>12000</medical>\n" +
            "            <provident-fund>15000</provident-fund>\n" +
            "        </salary>\n" +
            "    </staff>\n" +
            "</college>";
}
