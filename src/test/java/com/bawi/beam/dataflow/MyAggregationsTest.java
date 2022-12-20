package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MyAggregationsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);

    static class Log<T> extends PTransform<PCollection<T>, PCollection<T>> {
        static class LogFn<E> extends DoFn<E, E> {
            @ProcessElement
            public void process(@Element E element, OutputReceiver<E> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                LOGGER.info("Processing: {} ", String.format("%s (%s %s)", element, window.getClass().getSimpleName(), window.maxTimestamp()));
                receiver.output(element);
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
//            input.apply(MapElements.via(new SimpleFunction<T, T>() {
//                @Override
//                public T apply(T element) {
//                    LOGGER.info("Processing: {}", element);
//                    return element;
//                }
//            }));
            input.apply(ParDo.of(new LogFn<>()));
            return input;
        }
    }

    @Test
    public void testSumGlobally() {
        Pipeline pipeline = Pipeline.create();
        PCollection<Integer> result = pipeline.apply(Create.of(1, 2, 3))
                .apply(Sum.integersGlobally())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(6);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testCountGlobally() {
        Pipeline pipeline = Pipeline.create();
        PCollection<Long> result = pipeline.apply(Create.of("a", "b", "a"))
                .apply(Count.globally())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(3L);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testCountPerElement() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, Long>> result = pipeline.apply(Create.of("a", "b", "a"))
                .apply(Count.perElement())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 2L), KV.of("b", 1L)));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testTopNElements() {
        Pipeline pipeline = Pipeline.create();
        PCollection<Integer> result = pipeline.apply(Create.of(1, 3, 2, 4))
                .apply(Top.largest(2))
                .apply(Flatten.iterables());
        PAssert.that(result).containsInAnyOrder(3, 4);
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testGroupByKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, Iterable<Integer>>> result = pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(GroupByKey.create())
                .apply(new Log<>());
        PAssert.that(result).satisfies((SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void>) input -> {
            MatcherAssert.assertThat(input, Matchers.iterableWithSize(2));
            MatcherAssert.assertThat(input, Matchers
                    .either(Matchers.<KV<String, Iterable<Integer>>>hasItem(KV.of("a", List.of(1, 2))))
                    .or(Matchers.hasItem(KV.of("a", List.of(2, 1))))
            );
            MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("b", List.of(1))));
            return null;
        });
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void testCountPerKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Long>> result = pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(Count.perKey())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 2L), KV.of("b", 1L)));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSumPerKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Integer>> result = pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(Sum.integersPerKey())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 3), KV.of("b", 1)));
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void testMyCombineGlobally() {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(Combine.globally(new MyToDelimitedString<>()))
                .apply(new Log<>());
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testMyCombinePerKey() {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(Combine.perKey(new MyToDelimitedString<>()))
                .apply(new Log<>());
        pipeline.run().waitUntilFinish();
    }

    static class MyToDelimitedString<T> extends Combine.CombineFn<T, String, String> {
        private static final String DEFAULT_DELIMITER = ":";
        private final String delimiter;
        private final String accum = "";

        public MyToDelimitedString(String delimiter) {
            this.delimiter = delimiter;
        }

        public MyToDelimitedString() {
            this(DEFAULT_DELIMITER);
        }

        @Override
        public String createAccumulator() {
            return "";
        }

        @Override
        public String addInput(String mutableAccumulator, T input) {
            return mutableAccumulator.isEmpty() ? input.toString() : mutableAccumulator + delimiter + input.toString();
        }

        @Override
        public String mergeAccumulators(Iterable<String> accumulators) {
            StringBuilder acc = new StringBuilder(createAccumulator());
            for (String accumulator : accumulators) {
                if (acc.length() == 0)
                    acc = new StringBuilder(accumulator);
                else if (!accumulator.isEmpty()) {
                    acc.append(delimiter).append(accumulator);
                }
            }
            return acc.toString();
        }

        @Override
        public String extractOutput(String accumulator) {
            return accumulator;
        }
    }

}
