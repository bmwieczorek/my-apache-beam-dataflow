package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MyJoinUsingSideInputMapViewTest {

    private static final String TAG = "sideInputTag";

    static class MultiplyElementWithMappedValueVFn extends DoFn<KV<String, Integer>, KV<String, Integer>> {
        @ProcessElement
        public void process(@Element KV<String, Integer> element, @SideInput(TAG) Map<String, Integer> sideInputMap,
                            OutputReceiver<KV<String, Integer>> outputReceiver) {
            String key = element.getKey();
            Integer value = element.getValue();
            Integer multiplier = sideInputMap.getOrDefault(key, 1);
            KV<String, Integer> kv = KV.of(key, value * multiplier);
            System.out.println();
            outputReceiver.output(kv);
        }
    }

    @Test
    public void testJoinSideInputMapView() {
        // given
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, Integer>> main = pipeline.apply("Main", Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("b", 3)));
        PCollection<KV<String, Integer>> side = pipeline.apply("Side", Create.of(KV.of("a", 10), KV.of("b", 20), KV.of("c", 30)));

        // when
        PCollectionView<Map<String, Integer>> mapView = side.apply(View.asMap());
        PCollection<KV<String, Integer>> unwrapped = main.apply(ParDo.of(new MultiplyElementWithMappedValueVFn()).withSideInput(TAG, mapView));

        // then
        PAssert.that(unwrapped).containsInAnyOrder(List.of(KV.of("a", 10), KV.of("b", 40), KV.of("b", 60)));
        pipeline.run();
    }
}
