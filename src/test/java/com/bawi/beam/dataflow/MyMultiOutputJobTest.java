package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Test;

import static com.bawi.beam.dataflow.MyMultiOutputJob.*;

public class MyMultiOutputJobTest {

    @Test
    public void testMultiOutput() {
        Pipeline pipeline = Pipeline.create();

        PCollectionTuple multipleCollections = pipeline
                .apply(Create.of(1, 2, 3))
                .apply(new MyMultiOutputJob.MyMultiPTransform());

        PCollection<String> even = multipleCollections.get(evenTag).setCoder(StringUtf8Coder.of());
        PCollection<String> odd = multipleCollections.get(oddTag).setCoder(StringUtf8Coder.of());

        PAssert.that(even).containsInAnyOrder("2");
        PAssert.that(odd).containsInAnyOrder("1", "3");

        pipeline.run().waitUntilFinish();
    }
}
