package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class GenerateSequenceTest {
    public static void main(String[] args) {
        args = DataflowUtils.updateDataflowArgs(args);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<Long> apply = pipeline.apply(GenerateSequence
                .from(1)
                //.to(60) // bounded
                .withRate(20, Duration.standardSeconds(1L)));
        apply
                .apply(MyConsoleIO.write());
        PipelineResult run = pipeline.run();
//        run.waitUntilFinish();
    }
}
