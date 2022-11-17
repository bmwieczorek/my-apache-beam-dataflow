package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;

public class MyJoinUsingCoGroupByKeyTest {

    @Test
    public void testJoinUsingCoGroupByKey() {
        // given
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, Integer>> main = pipeline.apply("Main", Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("b", 3)));
        PCollection<KV<String, Integer>> side = pipeline.apply("Side", Create.of(KV.of("a", 10), KV.of("b", 20), KV.of("c", 30)));

        final TupleTag<Integer> mainTag = new TupleTag<>();
        final TupleTag<Integer> sideTag = new TupleTag<>();

        // PCollection with KV<K, ?> elements
        KeyedPCollectionTuple<String> keyedPCollectionTuple = KeyedPCollectionTuple.of(mainTag, main).and(sideTag, side);

        // Merge collection values into a CoGbkResult collection.
        PCollection<KV<String, CoGbkResult>> joinedCollection = keyedPCollectionTuple.apply(CoGroupByKey.create());

        // map values
        PCollection<KV<String, Integer>> unwrapped = joinedCollection.apply(
                FlatMapElements.into(kvs(strings(), integers())).via(input -> {
                        // System.out.println(input);
                        String key = input.getKey();
                        CoGbkResult value = input.getValue();
                        CoGbkResultSchema schema = value.getSchema();
                        TupleTagList tupleTagList = schema.getTupleTagList();

                        @SuppressWarnings("unchecked")
                        Iterable<Integer> mainElements = value.getAll((TupleTag<Integer>) tupleTagList.get(0));
                        @SuppressWarnings("unchecked")
                        Iterable<Integer> sideElements = value.getAll((TupleTag<Integer>) tupleTagList.get(1));

                        return StreamSupport.stream(mainElements.spliterator(), false)
                                .flatMap(m -> StreamSupport.stream(sideElements.spliterator(), false)
                                        .map(s -> KV.of(key, m * s))
                                )
                                .collect(Collectors.toList());

//                        List<KV<String, Integer>> list = new ArrayList<>();
//                        for (Integer mainElement : mainElements) {
//                            for (Integer sideElement : sideElements) {
//                                KV<String, Integer> kv = KV.of(key, mainElement * sideElement);
//                                System.out.println(kv);
//                                list.add(kv);
//                            }
//                        }
//                        return list;
                    }));

        PAssert.that(unwrapped).containsInAnyOrder(List.of(KV.of("a", 10), KV.of("b", 40), KV.of("b", 60)));

        pipeline.run();
    }
}
