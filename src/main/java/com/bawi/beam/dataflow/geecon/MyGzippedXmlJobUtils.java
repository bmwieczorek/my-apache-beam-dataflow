package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.VtdXmlParser.Entry;
import com.bawi.parser.StringLengthParser;
import com.bawi.parser.SumValuesParser;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.geecon.XmlPayload.XML_PAYLOAD;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.System.currentTimeMillis;
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

    public static class GunzipParseSalarySumGroupedXmls extends DoFn<KV<String, Iterable<byte[]>>, KV<String, Long>> {
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<Entry> mappingEntries = mappingEntries();
            vtdXmlParser = new VtdXmlParser(mappingEntries);
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

    public static class ParallelGzippedXmlSequence extends PTransform<PBegin, PCollection<KV<String, byte[]>>> {
        byte[] SINGLE_PAYLOAD_GZ;
        {
            try {
                SINGLE_PAYLOAD_GZ = gzip(XML_PAYLOAD);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private final long sequenceRate;

        public ParallelGzippedXmlSequence(long sequenceRate) {
            this.sequenceRate = sequenceRate;
        }

        @Override
        public PCollection<KV<String, byte[]>> expand(PBegin input) {
            return input.apply("ParallelSequence", GenerateSequence.from(0).withRate(sequenceRate, standardSeconds(1)).withTimestampFn(i -> now()))
                    .apply("ParGzippedXmlSeq", MapElements.into(kvs(strings(), of(byte[].class)))
                            .via(n -> KV.of(distribute(n), SINGLE_PAYLOAD_GZ)));
        }
    }

}
