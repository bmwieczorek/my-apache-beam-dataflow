package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NestedValueProviderTest {

    @Test
    public void shouldUseValueProvider() {
        Pipeline pipeline = Pipeline.create();

        int limit = 5;
        ValueProvider.NestedValueProvider<List<Integer>, Integer> nestedValueProvider = ValueProvider.NestedValueProvider.of(
                ValueProvider.StaticValueProvider.of(limit),
                l -> IntStream.rangeClosed(1, l).boxed().collect(Collectors.toList()));

        ListCoder<Integer> integerListCoder = ListCoder.of(SerializableCoder.of(Integer.class));

        PCollection<Integer> integersPCollection = pipeline
                .apply(Create.ofProvider(nestedValueProvider, integerListCoder))
                .apply(FlatMapElements.into(TypeDescriptors.integers()).via(iter -> iter));

        pipeline.run().waitUntilFinish();
        PAssert.that(integersPCollection).containsInAnyOrder(1, 2, 3, 4, 5);
    }
}
