package com.bawi.beam.dataflow.geecon;

import com.bawi.beam.dataflow.PipelineUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

import static com.bawi.beam.dataflow.PipelineUtils.*;
import static com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.distribute;
import static java.util.stream.StreamSupport.stream;
import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.joda.time.Duration.standardSeconds;
import static org.joda.time.Instant.now;

public class MyGBKStreamingJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyGBKStreamingJob.class);
    private static final String JOB_NAME = PipelineUtils.OWNER + "-" + MyGBKStreamingJob.class.getSimpleName().toLowerCase();

    public static void main(String[] args) throws IOException {
        String[] updatedArgs =
                isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME
                                , "--numWorkers=3"
                                , "--maxNumWorkers=3"
                                , "--workerMachineType=t2d-standard-2"
                                , "--sequenceRate=10"
                                , "--experiments=enable_stackdriver_agent_metrics"
                                , "--enableStreamingEngine=true"
                        ) :
                        PipelineUtils.updateArgs(args, "--sequenceRate=10");

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply(GenerateSequence.from(0).withRate(opts.getSequenceRate(), standardSeconds(1)).withTimestampFn(i -> now()))

                .apply("AddKeyToLong", MapElements.into(kvs(strings(), longs())).via(n -> KV.of(distribute(n), n)))

                .apply("Window1s", Window.into(FixedWindows.of(standardSeconds(1))))

                .apply(GroupByKey.create())

                .apply("LongsToString", ParDo.of(new DoFn<KV<String, Iterable<Long>>, KV<String, String>>() {

                    @ProcessElement
                    public void process(@DoFn.Element KV<String, Iterable<Long>> keyLongsIterable, OutputReceiver<KV<String, String>> receiver) {
                        String key = keyLongsIterable.getKey();
                        Iterable<Long> longsIterable = keyLongsIterable.getValue();
                        String joined = stream(longsIterable.spliterator(), false)
                                .peek(n -> Metrics.counter("keyToHostname", key + "_" + getHostname()).inc())
                                .peek(n -> LOGGER.info("keyToHostname {}", key + "_" + getHostname() + " for " + n))
                                .map(String::valueOf)
                                .collect(Collectors.joining(",")) ;
                        receiver.output(KV.of(key, joined));
                    }
                }))

                .apply("Log", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {

                    @ProcessElement
                    public void process(@Element KV<String, String> element, OutputReceiver<KV<String, String>> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                        String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
                        String msg = String.format("Processing '%s',ts=%s,w=%s,p=%s", element, timestamp, windowString, paneInfo);
                        LOGGER.info(msg);
                        receiver.output(element);
                    }
                }));

        pipeline.run();
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceRate();

        @SuppressWarnings("unused")
        void setSequenceRate(long value);
    }

}
