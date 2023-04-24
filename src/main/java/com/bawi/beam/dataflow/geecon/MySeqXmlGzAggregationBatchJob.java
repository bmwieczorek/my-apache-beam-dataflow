package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.io.SplitInputStream;
import com.bawi.parser.impl.StringLengthParser;
import com.bawi.parser.impl.SumValuesParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bawi.beam.dataflow.PipelineUtils.isDataflowRunnerOnClasspath;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.joining;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class MySeqXmlGzAggregationBatchJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySeqXmlGzAggregationBatchJob.class);
    private static final String JOB_NAME = "bartek-" + MySeqXmlGzAggregationBatchJob.class.getSimpleName().toLowerCase();

    public static void main(String[] args) throws IOException {

        String[] updatedArgs =
                isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--5000seq--1000-multi-xml-gz--e2st8"
//                                , "--jobName=" + JOB_NAME + "--5000000seq--1-single-xml-gz--e2st8"
//                                , "--jobName=" + JOB_NAME + "--5000000seq--1-single-xml-gz--t2dst4"
                                , "--numWorkers=1"
                                , "--maxNumWorkers=1"
                                , "--workerMachineType=e2-standard-8"
                                , "--sequenceLimit=5000"
//                                , "--sequenceLimit=5000000"
                        ) :
                        PipelineUtils.updateArgs(args
                                , "--sequenceLimit=3"
                        );

        byte[] SINGLE_PAYLOAD_GZ = gzip(PAYLOAD);
        byte[] MULTI_PAYLOAD_GZ = gzip(IntStream.range(0, 1000).boxed().map(i -> PAYLOAD).collect(joining("\n")));

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply(GenerateSequence.from(0).to(opts.getSequenceLimit()).withTimestampFn(i -> Instant.now()))
//                .apply("GenSingleXmlGzip", MapElements.into(of(byte[].class)).via(i -> SINGLE_PAYLOAD_GZ))
//                .apply("GunzipSingleXmlGzip", MapElements.into(strings()).via(bytes -> new String(gunzip(bytes))))

                .apply("GenMultiXmlGzip", MapElements.into(of(byte[].class)).via(i -> MULTI_PAYLOAD_GZ))
                .apply("GunzipSplitMultiXml", ParDo.of(new GunzipAndSplitMultiXml("<college", "</college>")))

                .apply("XmlVtdParse", ParDo.of(new XmlVtdParse()))
                .apply("SumSalariesPerXml", MapElements.into(integers())
                        .via(map -> parseInt(map.get("staff_basic_salary_sum"))))
                .apply("SumAllSalaries", Sum.integersGlobally().withoutDefaults())

//                .apply("LogSumAllSalaries", MapElements.into(voids()).via(sum -> {
//                    LOGGER.info("sumAllSalaries={}", sum);
//                    return null;
//                }))
                ;

        pipeline.run().waitUntilFinish();
    }

    private static class GunzipAndSplitMultiXml extends DoFn<byte[], String> {
        private final String startTag;
        private final String endTag;

        public GunzipAndSplitMultiXml(String startTag, String endTag) {
            this.startTag = startTag;
            this.endTag = endTag;
        }

        @ProcessElement
        public void process(@Element byte[] gzippedBytes, OutputReceiver<String> receiver) throws Exception {
            byte[] decompressedBytes = gunzip(gzippedBytes);
            try (InputStream is = new BufferedInputStream(new ByteArrayInputStream(decompressedBytes));
                SplitInputStream fwdStream = new SplitInputStream(is, startTag, endTag)) {
                byte[] splitBytes;
                while ((splitBytes = fwdStream.readXmlChunkAsBytes()) != null) {
                    String xml = new String(splitBytes);
                    receiver.output(xml);
                }
            }
        }
    }

    static class XmlVtdParse extends DoFn<String, Map<String, String>> {
        private static final Counter xmlsParsed = Metrics.counter(XmlVtdParse.class.getSimpleName(), "xmls-parsed");
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
        public void process(@Element String xml, OutputReceiver<Map<String, String>> receiver) throws Exception {
            Map<String, Object> map = vtdXmlParser.parseXml(xml);
            Map<String, String> remapped = map.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
            receiver.output(remapped);
            xmlsParsed.inc();
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceLimit();
        void setSequenceLimit(long value);
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
