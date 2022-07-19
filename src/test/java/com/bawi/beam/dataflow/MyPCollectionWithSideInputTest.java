package com.bawi.beam.dataflow;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.TreeSet;

public class MyPCollectionWithSideInputTest implements Serializable {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMainWithSideInput() {
        PCollection<KV<String, String>> main = pipeline.apply("Main", Create.of(KV.of("a", "1"), KV.of("b", "2")));

        PCollectionView<String> metaViewSideInput = pipeline.apply("Meta", Create.of("M")).apply(View.asSingleton());

        PCollection<KV<String, String>> result = main.apply(ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {

            @ProcessElement
            public void process(ProcessContext ctx) {
                KV<String, String> e = ctx.element();
                String meta = ctx.sideInput(metaViewSideInput);
                ctx.output(KV.of(e.getKey(), meta + e.getValue()));
            }
        }).withSideInputs(metaViewSideInput));

        // then
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", "M1"), KV.of("b", "M2")));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testMainWithSideInput2() {
        PCollectionView<List<List<String>>> metaListViewSideInput = pipeline
                .apply("Create", GenerateSequence.from(0).to(2))
                .apply("LongToString", ParDo.of(new DoFn<Long, List<String>>() {

                    @ProcessElement
                    public void process(ProcessContext c) {
                            Long e = c.element();
                            c.output(List.of(e + "", e + "" + e));
                    }
                }))
                .apply(View.asList());

        PCollection<KV<String, String>> main = pipeline.apply("Main", Create.of(KV.of("a", "1"), KV.of("b", "2")));

        PCollection<KV<String, String>> result = main.apply("MainProcessing", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {


            @ProcessElement
            public void process(ProcessContext ctx) {
                KV<String, String> e = ctx.element();
                List<List<String>> meta = ctx.sideInput(metaListViewSideInput);
                TreeSet<String> set = new TreeSet<>();
                meta.forEach(l -> set.add(l.toString()));
                ctx.output(KV.of(e.getKey(), set + "__"+ e.getValue()));
            }
        }).withSideInputs(metaListViewSideInput));

        // then
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", "[[0, 00], [1, 11]]__1"), KV.of("b", "[[0, 00], [1, 11]]__2")));
        pipeline.run().waitUntilFinish();
    }
}
