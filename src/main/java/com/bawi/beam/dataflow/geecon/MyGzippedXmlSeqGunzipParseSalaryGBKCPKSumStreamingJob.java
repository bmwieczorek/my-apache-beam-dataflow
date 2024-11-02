package com.bawi.beam.dataflow.geecon;

import com.bawi.VtdXmlParser;
import com.bawi.VtdXmlParser.Entry;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.MyPipelineOptions;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.ParallelRedistributedGzippedXmlUnboundedSequence;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bawi.beam.dataflow.LogUtils.hostname;
import static com.bawi.beam.dataflow.PipelineUtils.isDataflowRunnerOnClasspath;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.io.GzipUtils.gunzip;
import static java.lang.System.currentTimeMillis;
import static org.apache.beam.sdk.values.TypeDescriptors.voids;
import static org.joda.time.Duration.standardSeconds;

public class MyGzippedXmlSeqGunzipParseSalaryGBKCPKSumStreamingJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyGzippedXmlSeqGunzipParseSalaryGBKCPKSumStreamingJob.class);
    private static final String JOB_NAME = "bartek-" + MyGzippedXmlSeqGunzipParseSalaryGBKCPKSumStreamingJob.class.getSimpleName().toLowerCase();

    public static void main(String[] args) throws IOException {
        String[] updatedArgs = isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--50000-elem-per-sec-1-3x-t2dst2-10s-win"
                                , "--numWorkers=1"
                                , "--maxNumWorkers=3"
                                , "--autoscalingAlgorithm=THROUGHPUT_BASED"
                                , "--workerMachineType=t2d-standard-2"
                                , "--sequenceRate=50000"
                                , "--experiments=enable_stackdriver_agent_metrics"
                                , "--enableStreamingEngine=false"
                        ) :
                        PipelineUtils.updateArgs(args, "--sequenceRate=1");

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply("ParSeqOfGzippedXmls",  new ParallelRedistributedGzippedXmlUnboundedSequence(opts.getSequenceRate()))
                .apply("Window10secs",         Window.into(FixedWindows.of(standardSeconds(10))))

                .apply("GunzipParseXmlSalary",    ParDo.of(new GunzipParseXmlSalary()))

                .apply(GroupByKey.create())
                .apply("CombinePerKey(Sum)",     Combine.groupedValues(Sum.ofLongs()))
//                .apply(Combine.perKey(Sum.ofLongs())) // same as GroupByKey followed by Combine.groupedValues

                .apply("LogSumAllSalPKeyPWin",  MapElements.into(voids()).via(kv -> {LOGGER.info(kv.toString());return null;}));
        pipeline.run();
    }

    static class GunzipParseXmlSalary extends DoFn<KV<String, byte[]>, KV<String, Long>> {
        private VtdXmlParser vtdXmlParser;

        @Setup
        public void setup() {
            List<Entry> mappingEntries = MyGzippedXmlJobUtils.mappingEntries();
            vtdXmlParser = new VtdXmlParser(mappingEntries);
        }

        @ProcessElement
        public void process(@Element KV<String, byte[]> elem, OutputReceiver<KV<String, Long>> receiver) {
            String xml = new String(gunzip(elem.getValue()));
            Metrics.counter("keyToHostname", elem.getKey() + "_" + hostname()).inc();
            long start = currentTimeMillis();
            Map<String, Object> xmlAsMap = vtdXmlParser.parseXml(xml);
            Metrics.distribution(VtdXmlParser.class, "elapsedTime").update(currentTimeMillis() - start);
            long salariesSumInXml = (int) xmlAsMap.get("staff_basic_salary_sum");
            receiver.output(KV.of(elem.getKey(), salariesSumInXml));
        }
    }
}
