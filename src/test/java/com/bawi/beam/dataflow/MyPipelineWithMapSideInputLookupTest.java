package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;

import java.util.Map;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class MyPipelineWithMapSideInputLookupTest {

    private static final String TAG = "sideInputTag";

    static class FilterOutIfNotInLookupFn extends DoFn<String, String> {
        @ProcessElement
        public void process(@Element String element, @SideInput(TAG) Map<String, Void> sideInputMap,
                            OutputReceiver<String> outputReceiver) {
            if (sideInputMap.containsKey(element)) {
                outputReceiver.output(element);
            }
        }
    }

    @Test
    public void testJoinSideInputMapView() {
        // given
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply("Main", Create.of("a", "b", "c", "d"));

        PCollectionView<Map<String, Void>> refMapView = pipeline.apply("Ref", Create.of("b", "c", "b"))
                .apply(Deduplicate.values())
                .apply(MapElements.into(kvs(strings(), voids())).via(e -> KV.of(e, null)))
                .apply(View.asMap());

        // when
        PCollection<String> filtered = input.apply(ParDo.of(new FilterOutIfNotInLookupFn()).withSideInput(TAG, refMapView));

        // then
        PAssert.that(filtered).containsInAnyOrder("b", "c");
        pipeline.run();
    }
}
