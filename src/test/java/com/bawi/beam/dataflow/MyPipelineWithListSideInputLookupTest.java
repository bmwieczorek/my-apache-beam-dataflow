package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;

import java.util.List;

public class MyPipelineWithListSideInputLookupTest {

    private static final String TAG = "sideInputTag";

    static class FilterOutIfNotInLookupFn extends DoFn<String, String> {
        @ProcessElement
        public void process(@Element String element, @SideInput(TAG) List<String> sideInputList,
                            OutputReceiver<String> outputReceiver) {
            if (sideInputList.contains(element)) {
                outputReceiver.output(element);
            }
        }
    }

    @Test
    public void testJoinSideInputMapView() {
        // given
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply("Main", Create.of("a", "b", "c", "d"));

        PCollectionView<List<String>> refListView = pipeline.apply("Ref", Create.of("b", "c", "b"))
                .apply(Deduplicate.values())
                .apply(View.asList());

        // when
        PCollection<String> filtered = input.apply(ParDo.of(new FilterOutIfNotInLookupFn()).withSideInput(TAG, refListView));

        // then
        PAssert.that(filtered).containsInAnyOrder("b", "c");
        pipeline.run();
    }
}
