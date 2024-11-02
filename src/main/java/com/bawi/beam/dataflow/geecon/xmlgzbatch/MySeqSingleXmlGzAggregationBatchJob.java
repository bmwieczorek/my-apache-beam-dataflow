package com.bawi.beam.dataflow.geecon.xmlgzbatch;

import com.bawi.beam.dataflow.MyConsoleIO;
import com.bawi.beam.dataflow.PipelineUtils;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.ParallelGzippedXmlBoundedSequence;
import com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.XmlVtdParse;
import com.bawi.io.GzipUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

import java.io.IOException;

import static com.bawi.beam.dataflow.PipelineUtils.*;
import static com.bawi.beam.dataflow.geecon.MyGzippedXmlJobUtils.SINGLE_XML_PAYLOAD_GZ;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.longs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class
MySeqSingleXmlGzAggregationBatchJob {

    private static final String JOB_NAME = getJobNameWithOwner(MySeqSingleXmlGzAggregationBatchJob.class);

    public static void main(String[] args) throws IOException {

        String[] updatedArgs = isDataflowRunnerOnClasspath() ?
                        updateArgsWithDataflowRunner(args
                                , "--jobName=" + JOB_NAME + "--5000000seq--single-xml-gz--t2dst4"
                                , "--numWorkers=1"
                                , "--maxNumWorkers=2"
                                , "--workerMachineType=t2-standard-4"
                                , "--sequenceLimit=5000000"
                        ) : PipelineUtils.updateArgs(args, "--sequenceLimit=5000");

        MyPipelineOptions opts = PipelineOptionsFactory.fromArgs(updatedArgs).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        pipeline.apply("ParSeqOfMultiXmlsGz", new ParallelGzippedXmlBoundedSequence(opts.getSequenceLimit(), SINGLE_XML_PAYLOAD_GZ))

                .apply("GunzipXml", MapElements.into(of(byte[].class)).via(GzipUtils::gunzip))
                .apply("ToString", MapElements.into(strings()).via(String::new))

                .apply("XmlVtdParse", ParDo.of(new XmlVtdParse()))
                .apply("SumSalariesPerXml", MapElements.into(longs())
                        .via(xmlAsMap -> Long.parseLong(xmlAsMap.get("staff_basic_salary_sum"))))
                .apply("SumAllSalaries", Sum.longsGlobally().withoutDefaults())

                .apply(MyConsoleIO.write());

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
