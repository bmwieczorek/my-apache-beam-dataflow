package com.bawi.beam.dataflow.geecon.xmlgzbatch;

import com.bawi.beam.dataflow.MyConsoleIO;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.ParallelGzippedXmlBoundedSequence;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.SplitMultiXml;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.XmlVtdParseSumSalaries;
import com.bawi.io.GzipUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

import java.io.IOException;

import static com.bawi.beam.dataflow.PipelineUtils.*;
import static com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.MULTI_XML_PAYLOAD_GZ;
import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class MySeqMultiXmlGzAggregationBatchJob {

    private static final String JOB_NAME = getJobNameWithOwner(MySeqMultiXmlGzAggregationBatchJob.class);

    public static void main(String[] args) throws IOException {

        String[] updatedArgs = isDataflowRunnerOnClasspath() ?
                updateArgsWithDataflowRunner(args
                        , "--jobName=" + JOB_NAME + "--50000seq--1000-multi-xml-gz--t2dst4"
                        , "--numWorkers=1"
                        , "--maxNumWorkers=2"
                        , "--workerMachineType=t2d-standard-4"
                        , "--sequenceLimit=" + 50 * 1000
                ) : PipelineUtils.updateArgs(args, "--sequenceLimit=5");

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply("ParSeqOfMultiXmlsGz", new ParallelGzippedXmlBoundedSequence(opts.getSequenceLimit(),
                                                                        MULTI_XML_PAYLOAD_GZ))
                .apply("GunzipXml", MapElements.into(of(byte[].class)).via(GzipUtils::gunzip))
                .apply("SplitMultiXmlToStrings", ParDo.of(new SplitMultiXml("<college", "</college>")))

                .apply("XmlVtdParseSumSalaries", ParDo.of(new XmlVtdParseSumSalaries()))

                .apply("Combine.globally(Sum)", Combine.globally(Sum.ofLongs()).withoutDefaults())

                .apply("LogGlobalSalariesSum", MyConsoleIO.write());

        PipelineResult pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();
        logMetrics(pipelineResult);
    }

    public interface MyPipelineOptions extends PipelineOptions {
        @Validation.Required
        long getSequenceLimit();
        void setSequenceLimit(long value);
    }
}
