package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

public class BeamBuiltInFunctionsTest implements Serializable {

    @Test
    public void shouldMergeAkaFlattenPCollection() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> lowerCasePColl = pipeline.apply("Lower", Create.of("a", "b", "c", "d"));
        PCollection<String> upperCasePColl = pipeline.apply("Upper", Create.of("X", "Y", "Z"));
        PCollection<String> numberAsStringPColl = pipeline.apply("Numbers", Create.of("2","1"));
        PCollectionList<String> pCollectionList = PCollectionList.of(lowerCasePColl).and(upperCasePColl).and(numberAsStringPColl);

        PCollection<String> merged = pCollectionList.apply(Flatten.pCollections());

        PAssert.that(merged).containsInAnyOrder("a", "1", "b", "c", "d", "2", "X", "Y", "Z");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldPartitionPCollection() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply(Create.of("a", "1", "b", "c", "d", "2", "X", "Y", "Z"));

        PCollectionList<String> pCollectionList = pCollection.apply(Partition.of(3, (element, numPartitions) -> {
            try {
                Integer.parseInt(element);
                return 2;
            } catch (NumberFormatException e) {
                return element.equals(element.toLowerCase()) ? 0 : 1;
            }
        }));

        PAssert.that(pCollectionList.get(0)).satisfies((SerializableFunction<Iterable<String>, Void>) input -> null);
        PAssert.that(pCollectionList.get(0)).containsInAnyOrder("a", "b", "c", "d");
        PAssert.that(pCollectionList.get(1)).containsInAnyOrder("X", "Y", "Z");
        PAssert.that(pCollectionList.get(2)).containsInAnyOrder("1", "2");

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldAddKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply(Create.of("a", "b", "c"));
        PCollection<KV<String, String>> kvPCollection = pCollection.apply(WithKeys.of(new SerializableFunction<String, String>() {
            @Override
            public String apply(String input) {
                return input.toUpperCase();
            }
        }));
        PAssert.that(kvPCollection).containsInAnyOrder(List.of(KV.of("A", "a"), KV.of("B", "b"), KV.of("C", "c")));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldExtractKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> kvpCollection = pipeline.apply(Create.of(KV.of("A", "a"), KV.of("B", "b")));

        PCollection<String> pCollection = kvpCollection.apply(Keys.create());

        PAssert.that(pCollection).satisfies(input -> null);
        PAssert.that(pCollection).containsInAnyOrder("A", "B");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldExtractValue() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> kvpCollection = pipeline.apply(Create.of(KV.of("A", "a"), KV.of("B", "b")));

        PCollection<String> pCollection = kvpCollection.apply(Values.create());

        PAssert.that(pCollection).satisfies(input -> null);
        PAssert.that(pCollection).containsInAnyOrder("a", "b");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldFilter() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply("Lower", Create.of("a", "b", "c", "d"));

        PCollection<String> filtered = pCollection.apply(Filter.by(s -> !"a".equals(s)));

        PAssert.that(filtered).containsInAnyOrder("c", "b", "d");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldFlatMap() {
        Pipeline pipeline = Pipeline.create();
        PCollection<List<String>> pCollection = pipeline.apply(Create.of(List.of(List.of("a", "b"), List.of("c", "d"))));

        PCollection<String> flatMapped = pCollection.apply(FlatMapElements.into(TypeDescriptors.strings()).via(list -> list));
//        PCollection<String> flatMapped = pCollection.apply(FlatMapElements.via(new SimpleFunction<List<String>, Iterable<String>>() {
//            @Override
//            public Iterable<String> apply(List<String> input) {
//                return input;
//            }
//        }));

        PAssert.that(flatMapped).satisfies(input -> null);
        PAssert.that(flatMapped).containsInAnyOrder("a", "c", "b", "d");
        pipeline.run().waitUntilFinish();
    }

}
