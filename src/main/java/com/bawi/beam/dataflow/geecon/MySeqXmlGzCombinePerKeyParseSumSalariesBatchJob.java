package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.VtdXmlParser.Entry;
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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.LogUtils.hostnameAndThreadId;
import static com.bawi.beam.dataflow.PipelineUtils.*;
import static com.bawi.beam.dataflow.geecon.XmlPayload.XML_PAYLOAD;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class MySeqXmlGzCombinePerKeyParseSumSalariesBatchJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySeqXmlGzCombinePerKeyParseSumSalariesBatchJob.class);
    private static final String JOB_NAME = "bartek-" + MySeqXmlGzCombinePerKeyParseSumSalariesBatchJob.class.getSimpleName().toLowerCase();

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

                .apply("SumSalariesPKCombine", Combine.perKey(new GunzipParseSumSalariesInXmlCombineFn()))

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

    static class GunzipParseSumSalariesInXmlCombineFn extends Combine.CombineFn<byte[], MySumAccum, Long> {
        private final List<Entry> mapping = List.of(
                new Entry("college_first_staff_dep_name", "staff[1]/@dep_name", String.class),
                new Entry("college_first_staff_dep_name_length", "staff[1]/@dep_name", StringLengthParser.class),
                new Entry("staff_basic_salary_sum", "staff/salary/basic", SumValuesParser.class),
                new Entry("staff_id_attr_sum", "staff/@id", SumValuesParser.class),
                new Entry("college_id", "@id", Integer.class)
        );

        @Override
        public MySumAccum createAccumulator() {
            Metrics.counter("MySumAccum", "createAccumulator_" + hostnameAndThreadId()).inc();
            return new MySumAccum(0L);
        }

        @Override
        public MySumAccum addInput(MySumAccum mutableAccumulator, byte[] input) {
            Metrics.counter("MySumAccum", "addInput_" + hostname()).inc();
            long start = currentTimeMillis();
            String xml = new String(gunzip(input));
            Map<String, Object> xmlAsMap = new VtdXmlParser(mapping).parseXml(xml);
            long staffBasicSalarySum = (int) xmlAsMap.get("staff_basic_salary_sum");
            Metrics.distribution(MySeqXmlGzCombinePerKeyParseSumSalariesBatchJob.class, "elapsedTime").update(currentTimeMillis() - start);
            return new MySumAccum(staffBasicSalarySum);
        }

        @Override
        public MySumAccum mergeAccumulators(Iterable<MySumAccum> accumulators) {
            Metrics.counter("MySumAccum", "mergeAccumulators_" + hostname()).inc();
            long sum = 0L;
            for (MySumAccum accumulator : accumulators) {
                sum += accumulator.getSum();
            }
            return new MySumAccum(sum);
        }

        @Override
        public Long extractOutput(MySumAccum accumulator) {
            Metrics.counter("MySumAccum", "extractOutput_" + hostname()).inc();
            return accumulator.getSum();
        }
    }

    static class MySumAccum implements Serializable {
        private final long sum;

        public MySumAccum(long sum) {
            this.sum = sum;
        }

        public long getSum() {
            return sum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MySumAccum that = (MySumAccum) o;
            return sum == that.sum;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sum);
        }

        @Override
        public String toString() {
            return "MySumAccum{sum=" + sum + '}';
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceLimit();

        @SuppressWarnings("unused")
        void setSequenceLimit(long value);
    }
}
