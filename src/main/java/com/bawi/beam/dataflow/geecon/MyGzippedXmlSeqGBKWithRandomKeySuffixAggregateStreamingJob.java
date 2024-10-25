package com.bawi.beam.dataflow.geecon;

import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.beam.dataflow.geecon.MySeqXmlGzJobUtils.GunzipParseGroupedXmls;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static com.bawi.beam.dataflow.PipelineUtils.isDataflowRunnerOnClasspath;
import static com.bawi.beam.dataflow.PipelineUtils.updateArgsWithDataflowRunner;
import static com.bawi.beam.dataflow.geecon.MySeqXmlGzJobUtils.GenGzippedXmls;
import static com.bawi.beam.dataflow.geecon.MySeqXmlGzJobUtils.MyPipelineOptions;
import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.joda.time.Duration.standardSeconds;

public class MyGzippedXmlSeqGBKWithRandomKeySuffixAggregateStreamingJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyGzippedXmlSeqGBKWithRandomKeySuffixAggregateStreamingJob.class);
    private static final String JOB_NAME = "bartek-" + MyGzippedXmlSeqGBKWithRandomKeySuffixAggregateStreamingJob.class.getSimpleName().toLowerCase();

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

                .apply("AddKeyRandomSuffix",   ParDo.of(new DoFn<KV<String, byte[]>, KV<String, byte[]>>() {
                    private final Random random = new Random();
                    @ProcessElement
                    public void process(ProcessContext ctx) {
                        String newKey = ctx.element().getKey() + "/" + random.nextInt(10);
                        ctx.output(KV.of(newKey, ctx.element().getValue()));
                    }
                }))

                .apply(GroupByKey.create()) // new stage

                .apply("GunzipParsXmlSalSumGrp",ParDo.of(new GunzipParseGroupedXmls()))

                .apply("RemoveKeyRandomSuffix", MapElements.into(kvs(strings(), longs())).via(kv -> {String originalKey = kv.getKey().substring(0, kv.getKey().indexOf("/"));return KV.of(originalKey, kv.getValue());}))

                .apply("CombinePerKey(Sum)",    Combine.perKey(Sum.ofLongs())) // new stage

                .apply("LogSumAllSalPKeyPWin",  MapElements.into(voids()).via(kv -> {LOGGER.info(kv.toString());return null;}));
        pipeline.run();
    }

}
