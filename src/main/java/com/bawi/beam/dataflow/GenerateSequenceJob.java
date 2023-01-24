package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class GenerateSequenceJob {
    public static void main(String[] args) {
        args = PipelineUtils.updateArgsWithDataflowRunner(args);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // generate numbers from 0 to 5999 and send each number every ~1/10s
        PCollection<Long> numbers = pipeline.apply(org.apache.beam.sdk.io.GenerateSequence
                .from(0)
                .to(6000) // exclusive
                .withRate(10, Duration.standardSeconds(1L)));

        numbers
                .apply(MyConsoleIO.write());
        PipelineResult run = pipeline.run();
        run.waitUntilFinish();
    }
}
