package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Test;

public class MyMergePCollectionsIntoOneUsingFlattenTest {

    @Test
    public void shouldUseValueProvider() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> lowerCasePColl = pipeline.apply("Lower", Create.of("a", "b", "c", "d"));
        PCollection<String> upperCasePColl = pipeline.apply("Upper", Create.of("X", "Y", "Z"));
        PCollection<String> numberAsStringPColl = pipeline.apply("Numbers", Create.of("2","1"));
        PCollectionList<String> pCollectionList = PCollectionList.of(lowerCasePColl).and(upperCasePColl).and(numberAsStringPColl);

        PCollection<String> merged = pCollectionList.apply(Flatten.pCollections());

        PAssert.that(merged).containsInAnyOrder("a", "1", "b", "c", "d", "2", "X", "Y", "Z");
        pipeline.run().waitUntilFinish();
    }
}
