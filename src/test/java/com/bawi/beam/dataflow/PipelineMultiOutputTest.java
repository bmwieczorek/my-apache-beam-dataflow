package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;

public class PipelineMultiOutputTest {

    static TupleTag<Integer> positiveTag = new TupleTag<>("positive");
    static TupleTag<Integer> zeroTag = new TupleTag<>("zero");
    static TupleTag<Integer> negativeTag = new TupleTag<>("negative");

    static class DispatchingFn extends DoFn<Integer, Integer> {
        @ProcessElement
        public void process(@Element Integer element, MultiOutputReceiver multiOutputReceiver) {
            if (element > 0) multiOutputReceiver.get(positiveTag).output(element);
            if (element == 0) multiOutputReceiver.get(zeroTag).output(element);
            if (element < 0) multiOutputReceiver.get(negativeTag).output(element);
        }
    }

    @Test
    public void testMultiOutput() {
        Pipeline pipeline = Pipeline.create();

        PCollectionTuple multipleCollections = pipeline
                .apply(Create.of(1, -1, 2, 0, -2))
                .apply(ParDo.of(new DispatchingFn())
                            .withOutputTags(positiveTag, TupleTagList.of(zeroTag).and(negativeTag))
                );

        PCollection<Integer> positive = multipleCollections.get(positiveTag).setCoder(VarIntCoder.of());
        PCollection<Integer> zero = multipleCollections.get(zeroTag).setCoder(VarIntCoder.of());
        PCollection<Integer> negative = multipleCollections.get(negativeTag).setCoder(VarIntCoder.of());

        PAssert.that(positive).containsInAnyOrder(1, 2);
        PAssert.that(zero).containsInAnyOrder(0);
        PAssert.that(negative).containsInAnyOrder(-1, -2);

        pipeline.run().waitUntilFinish();
    }
}
