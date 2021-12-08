package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PipelineWithFailureTest {

    @Test
    public void shouldProcessWithFailure() {
        Pipeline pipeline = Pipeline.create();

        WithFailures.Result<PCollection<Integer>, String> result = pipeline
                .apply(Create.of("1", "a", "2"))
                .apply(MapElements
                        .into(TypeDescriptors.integers())
                        .via((SerializableFunction<String, Integer>) Integer::parseInt)
                        .exceptionsInto(TypeDescriptors.strings())
                        .exceptionsVia((WithFailures.ExceptionElement<String> ee) -> ee.element() + ee.exception().getMessage()));

        PCollection<Integer> output = result.output();
        PAssert.that(output).containsInAnyOrder(1, 2);

        PCollection<String> failures = result.failures();
        PAssert.that(failures).containsInAnyOrder("a" + "For input string: \"a\"");

        pipeline.run().waitUntilFinish();
    }


    public static final TupleTag<Integer> successTag = new TupleTag<>() {};
    public static final TupleTag<String> errorTag = new TupleTag<>() {};

    static class ParserFn extends DoFn<String, Integer> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            String element = context.element();
            System.out.println("Processing element " + element);
            try {
                Integer i = Integer.parseInt(element);
                System.out.println("Processed element " + element);
                context.output(successTag, i);
            } catch (Exception e) {
                System.out.println("Failed to process element " + element + " due to " + e.getMessage());
                context.output(errorTag, element + e.getMessage());
            }
        }
    }

    static class MyPTransformWithFailures extends PTransform<PCollection<String>, WithFailures.Result<PCollection<Integer>, String>> {

        @Override
        public WithFailures.Result<PCollection<Integer>, String> expand(PCollection<String> input) {
            PCollectionTuple result = input.apply(ParDo.of(new ParserFn()).withOutputTags(successTag, TupleTagList.of(errorTag)));
            return WithFailures.Result.of(result.get(successTag), result.get(errorTag));
        }
    }

    @Test
    public void shouldProcessWithFailure2() {
        Pipeline pipeline = Pipeline.create();

        List<PCollection<String>> failureCollection = new ArrayList<>();
        PCollection<Integer> output = pipeline
                .apply(Create.of("1", "a", "2"))
                .apply(new MyPTransformWithFailures()).failuresTo(failureCollection).setCoder(VarIntCoder.of());

        PCollection<String> failures = PCollectionList.of(failureCollection)
                .apply(Flatten.pCollections())
                .setCoder(StringUtf8Coder.of());

        PAssert.that(output).containsInAnyOrder(1, 2);
        PAssert.that(failures).containsInAnyOrder("a" + "For input string: \"a\"");

        pipeline.run().waitUntilFinish();
    }
}
