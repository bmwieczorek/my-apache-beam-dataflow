package com.bawi.beam.dataflow.geecon;

import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.GenGzippedXmls;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.GunzipParseSalarySumGroupedXmls;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.MyPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.bawi.beam.dataflow.PipelineUtils.isDataflowRunnerOnClasspath;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static org.apache.beam.sdk.values.TypeDescriptors.voids;
import static org.joda.time.Duration.standardSeconds;

public class MyGzippedXmlSeqGBKGunzipParseSumStreamingJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyGzippedXmlSeqGBKGunzipParseSumStreamingJob.class);
    private static final String JOB_NAME = "bartek-" + MyGzippedXmlSeqGBKGunzipParseSumStreamingJob.class.getSimpleName().toLowerCase();

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

        pipeline.apply("ParSeqOfGzippedXmls",  new GenGzippedXmls(opts.getSequenceRate()))
                .apply("Window10secs",         Window.into(FixedWindows.of(standardSeconds(10))))

                .apply(GroupByKey.create()) // new stage

                .apply("GunzipParsXmlSalSumGrp", ParDo.of(new GunzipParseSalarySumGroupedXmls()))

                .apply("LogSumAllSalPKeyPWin",  MapElements.into(voids()).via(kv -> {LOGGER.info(kv.toString());return null;}));
        pipeline.run();
    }

}
