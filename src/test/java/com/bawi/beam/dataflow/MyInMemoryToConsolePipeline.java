package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class MyInMemoryToConsolePipeline {

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PTransform<PBegin, PCollection<String>> createStringsPT =
                Create.of("a", "bb", "ccc", "dddd");

        PCollection<String> inMemPC = pipeline.apply(createStringsPT);

        PTransform<PCollection<String>, PCollection<String>> filterPT
                = Filter.by(s -> s.length() > 1);

        inMemPC.apply(filterPT)

                // MapElements<InputT, OutputT> extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>>
//                .apply(MapElements
//                        .into(TypeDescriptors.integers()) // requires specifying TypeDescriptor
//                        .via( // allow using lambda
//                                s -> {
//                                    System.out.println("s=" + s);
//                                    return s.length();
//                                }
//                        )
//                )

// or
                .apply(MapElements.via(new SimpleFunction<String, Integer>() {
                    @Override
                    public Integer apply(String s) { // does not implement functional interface but TypeDescriptor not required
                        System.out.println("s=" + s);
                        return s.length();
                    }
                }))

// or

                // ParDo.of returning SingleOutput<InputT, OutputT> extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>>
//                .apply(ParDo.of(new DoFn<String, Integer>() {
//                    @ProcessElement
//                    public void processElement(@Element String element, OutputReceiver<Integer> out) {
//                        System.out.println("element=" + element);
//                        out.output(element.length());
//                    }
//                }))

// or with ProcessContext instead of @Element and OutputReceiver
//                .apply(ParDo.of(new DoFn<String, Integer>() { // ParDo.of extends PTransform
//                    @ProcessElement
//                    public void processElement(ProcessContext context) {
//                        String element = context.element();
//                        System.out.println("element=" + element);
//                        context.output(element.length());
//                    }
//                }))

                // custom impl of PTransform
                //  - with nested transformation that prints input to console
                //  - that returns PDone disallowing further transformation (no apply method available)
                .apply(MyConsoleIO.write())
        ;

        pipeline.run().waitUntilFinish();
    }

}
