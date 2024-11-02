package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.VtdXmlParser.Entry;
import com.bawi.io.SplitInputStream;
import com.bawi.parser.StringLengthParser;
import com.bawi.parser.SumValuesParser;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.geecon.XmlPayload.XML_PAYLOAD;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.joining;
import static java.util.stream.StreamSupport.stream;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.joda.time.Duration.standardSeconds;
import static org.joda.time.Instant.now;

public class MyGzippedXmlJobUtils {

    public static String distribute(Long n) {
        return List.of("US", "US", "PL", "US", "MT", "US", "US", "PL", "US", "US").get((int) (n % 10));
    }

    public static class SplitMultiXml extends DoFn<byte[], String> {
        private final String startTag;
        private final String endTag;

        public SplitMultiXml(String startTag, String endTag) {
            this.startTag = startTag;
            this.endTag = endTag;
        }

        @ProcessElement
        public void process(@Element byte[] bytes, OutputReceiver<String> receiver) throws Exception {
            try (InputStream is = new BufferedInputStream(new ByteArrayInputStream(bytes));
                 SplitInputStream fwdStream = new SplitInputStream(is, startTag, endTag)) {
                byte[] splitBytes;
                while ((splitBytes = fwdStream.readXmlChunkAsBytes()) != null) {
                    String xml = new String(splitBytes);
                    receiver.output(xml);
                }
            }
        }
    }

    public static List<Entry> mappingEntries() {
        return List.of(
                new Entry("college_first_staff_dep_name", "staff[1]/@dep_name", String.class),
                new Entry("college_first_staff_dep_name_length", "staff[1]/@dep_name", StringLengthParser.class),
                new Entry("staff_basic_salary_sum", "staff/salary/basic", SumValuesParser.class),
                new Entry("staff_id_attr_sum", "staff/@id", SumValuesParser.class),
                new Entry("college_id", "@id", Integer.class)
        );
    }

    public static class XmlVtdParse extends DoFn<String, Map<String, String>> {
        private static final Counter xmlsParsed = Metrics.counter(XmlVtdParse.class.getSimpleName(), "xmls-parsed");
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<Entry> mappingEntries = mappingEntries();
            vtdXmlParser = new VtdXmlParser(mappingEntries);
        }

        @ProcessElement
        public void process(@Element String xml, OutputReceiver<Map<String, String>> receiver) throws Exception {
            Map<String, Object> parsedXmlAsMap = vtdXmlParser.parseXml(xml);
            Map<String, String> remapped = parsedXmlAsMap.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
            receiver.output(remapped);
            xmlsParsed.inc();
        }
    }

    public static class XmlVtdParseSumSalaries extends DoFn<String, Long> {
        private static final Counter xmlsParsed = Metrics.counter(XmlVtdParse.class.getSimpleName(), "xmls-parsed");
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<Entry> mappingEntries = mappingEntries();
            vtdXmlParser = new VtdXmlParser(mappingEntries);
        }

        @ProcessElement
        public void process(@Element String xml, OutputReceiver<Long> outputReceiver) throws Exception {
            Map<String, Object> xmlAsMap = vtdXmlParser.parseXml(xml);
            xmlsParsed.inc();
            long staffBasicSalarySum = (long) (int) xmlAsMap.get("staff_basic_salary_sum");
            outputReceiver.output(staffBasicSalarySum);
        }
    }

    public static class GunzipParseSalarySumGroupedXmls extends DoFn<KV<String, Iterable<byte[]>>, KV<String, Long>> {
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<Entry> mappingEntries = mappingEntries();
            vtdXmlParser = new VtdXmlParser(mappingEntries);
        }

        @ProcessElement
        public void process(@Element KV<String, Iterable<byte[]>> elem, OutputReceiver<KV<String, Long>> receiver) {
            long salariesSumInXmlsGroup = stream(elem.getValue().spliterator(), false)
                    .map(bytes -> new String(gunzip(bytes)))

                    .peek(xml -> Metrics.counter("keyToHostname", elem.getKey() + "__" + hostname()).inc())
                    .map(xml -> {
                        long start = currentTimeMillis();
                        Map<String, Object> xmlAsMap = vtdXmlParser.parseXml(xml);
                        Metrics.distribution(VtdXmlParser.class, "elapsedTime").update(currentTimeMillis() - start);
                        return xmlAsMap;
                    })

                    .map(xmlAsMap -> (long)(int) xmlAsMap.get("staff_basic_salary_sum"))
                    .reduce(0L, Long::sum);
            receiver.output(KV.of(elem.getKey(), salariesSumInXmlsGroup));
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceRate();

        @SuppressWarnings("unused")
        void setSequenceRate(long value);
    }

    public static byte[] SINGLE_XML_PAYLOAD_GZ = gzip(XML_PAYLOAD);
    public static byte[] MULTI_XML_PAYLOAD_GZ = gzip(IntStream.range(0, 1000).boxed().map(i -> XML_PAYLOAD).collect(joining("\n")));

    public static class ParallelRedistributedGzippedXmlUnboundedSequence extends PTransform<PBegin, PCollection<KV<String, byte[]>>> {

        private final long sequenceRate;

        public ParallelRedistributedGzippedXmlUnboundedSequence(long sequenceRate) {
            this.sequenceRate = sequenceRate;
        }

        @Override
        public PCollection<KV<String, byte[]>> expand(PBegin input) {
            return input.apply("ParallelUnboundedSequence", GenerateSequence.from(0).withRate(sequenceRate, standardSeconds(1)).withTimestampFn(i -> now()))
                    .apply("AddRedistributeXmlGz", MapElements.into(kvs(strings(), of(byte[].class)))
                            .via(n -> KV.of(distribute(n), SINGLE_XML_PAYLOAD_GZ)));
        }
    }

    public static class ParallelRedistributedGzippedXmlBoundedSequence extends PTransform<PBegin, PCollection<KV<String, byte[]>>> {
        private final long sequenceLimit;
        private final byte[] xmlGzBytes;

        public ParallelRedistributedGzippedXmlBoundedSequence(long sequenceLimit, byte[] xmlGzBytes) {
            this.sequenceLimit = sequenceLimit;
            this.xmlGzBytes = xmlGzBytes;
        }

        @Override
        public PCollection<KV<String, byte[]>> expand(PBegin input) {
            return input.apply("ParallelBoundedSequence", GenerateSequence.from(0).to(sequenceLimit).withTimestampFn(i -> now()))
                    .apply("ToRedistributedXmlGz", MapElements.into(kvs(strings(), of(byte[].class)))
                            .via(n -> KV.of(distribute(n), xmlGzBytes)));
        }
    }

    public static class ParallelGzippedXmlBoundedSequence extends PTransform<PBegin, PCollection<byte[]>> {
        private final long sequenceLimit;
        private final byte[] xmlGzBytes;

        public ParallelGzippedXmlBoundedSequence(long sequenceLimit, byte[] xmlGzBytes) {
            this.sequenceLimit = sequenceLimit;
            this.xmlGzBytes = xmlGzBytes;
        }

        @Override
        public PCollection<byte[]> expand(PBegin input) {
            return input.apply("ParallelBoundedSequence", GenerateSequence.from(0).to(sequenceLimit).withTimestampFn(i -> now()))
                    .apply("ToXmlGz", MapElements.into(of(byte[].class)).via(n -> xmlGzBytes));
        }
    }

}
