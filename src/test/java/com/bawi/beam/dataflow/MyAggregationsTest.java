package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.bawi.beam.dataflow.LogUtils.windowToString;

public class MyAggregationsTest {
    private static Pipeline getPipeline(Runner runner) {
        return runner == Runner.Direct ?
                Pipeline.create() :
                Pipeline.create(PipelineOptionsFactory.fromArgs(PipelineUtils.updateArgsWithDataflowRunner()).create());
    }

    @Test
    public void testNoAggregation() {
        Runner runner = Runner.Direct;
//        Runner runner = Runner.Dataflow;
        Pipeline pipeline = getPipeline(runner);

        PCollection<Integer> result = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6))
                .apply(new Log<>());

        if (isDirect(runner)) {
            PAssert.that(result).containsInAnyOrder(1, 2, 3, 4, 5, 6);
        }

        pipeline.run().waitUntilFinish();
    }

    private static boolean isDirect(Runner runner) {
        return runner == Runner.Direct;
    }

    @Test
    public void testSumGlobally() {
        Pipeline pipeline = getPipeline(Runner.Direct);
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
        Runner runner = Runner.Direct;
//        Runner runner = Runner.Dataflow;
        Pipeline pipeline = getPipeline(runner);
        PCollection<KV<String, Iterable<Integer>>> result = pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                .apply(GroupByKey.create())
                .apply(new Log<>());
        if (isDirect(runner)) {
            PAssert.that(result).satisfies((SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void>) input -> {
                MatcherAssert.assertThat(input, Matchers.iterableWithSize(2));
                MatcherAssert.assertThat(input, Matchers
                        .either(Matchers.<KV<String, Iterable<Integer>>>hasItem(KV.of("a", List.of(1, 2))))
                        .or(Matchers.hasItem(KV.of("a", List.of(2, 1))))
                );
                MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("b", List.of(1))));
                return null;
            });
        }
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void testCountPerKey() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Long>> result = pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 1), KV.of("a", 2)))
                // same as Combine.perKey(new CountFn<V>());
                .apply(Count.perKey())
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 2L), KV.of("b", 1L)));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testCombinePerKeyWithSum() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Integer>> result =
                pipeline.apply(Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 10), KV.of("b", 20)))
                        // same as Sum.integersPerKey()
                        .apply(Combine.perKey(Sum.ofIntegers()))
                        .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 3), KV.of("b", 30)));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testGroupedValues() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Integer>> result =
        pipeline.apply(Create.<KV<String, Iterable<Integer>>>of(KV.of("a", List.of(1, 2)), KV.of("b", List.of(10, 20)))
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(VarIntCoder.of()))))
                .apply(Combine.groupedValues(Sum.ofIntegers()))
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 3), KV.of("b", 30)));
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void testGroupPerKeyAndGroupedValues() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String,Integer>> result =
                pipeline.apply(Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 10), KV.of("b", 20)))
                        .apply(GroupByKey.create())
                        .apply(Combine.groupedValues(Sum.ofIntegers()))
                        .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(List.of(KV.of("a", 3), KV.of("b", 30)));
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
        PCollection<Integer> result = pipeline.apply(Create.of(1, 2, 3, 4))
                .apply(Combine.globally(Sum.ofIntegers()))
                .apply(new Log<>());// 10
        PAssert.that(result).containsInAnyOrder(10);
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void testMyCombineWindowGlobally() {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.timestamped(
                        // W1 [0-5) s
                        TimestampedValue.of(1, Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(2, Instant.parse("2021-12-16T00:00:01Z")),

                        // W2 [5,10) s
                        TimestampedValue.of(10, Instant.parse("2021-12-16T00:00:05Z")),
                        TimestampedValue.of(20, Instant.parse("2021-12-16T00:00:06Z"))
                ))
                .apply(Combine.globally(Sum.ofIntegers())) // uses by default global window so sum of all elements despite their timestamps
                .apply(new Log<>()); //
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testMyCombineWindow() {
        Pipeline pipeline = Pipeline.create();
        PCollection<Integer> result = pipeline.apply(Create.timestamped(
                        // W1 [0-5) s
                        TimestampedValue.of(1, Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(2, Instant.parse("2021-12-16T00:00:01Z")),

                        // W2 [5,10) s
                        TimestampedValue.of(10, Instant.parse("2021-12-16T00:00:05Z")),
                        TimestampedValue.of(20, Instant.parse("2021-12-16T00:00:06Z"))
                ))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
//                .apply(Window.into(new GlobalWindows())) // re-window to global or bigger window would aggregate all elements in all windows
                .apply(Sum.integersGlobally().withoutDefaults()) // sum elements within each window separately
                .apply(new Log<>());
        PAssert.that(result).containsInAnyOrder(3, 30);
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
            return mutableAccumulator == null || mutableAccumulator.isEmpty() ? input.toString() : mutableAccumulator + delimiter + input.toString();
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

    private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);

    static class Log<T> extends PTransform<PCollection<T>, PCollection<T>> {
        static class LogFn<E> extends DoFn<E, E> {
            @ProcessElement
            public void process(@Element E element, OutputReceiver<E> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                LOGGER.info("Processing: {} ", String.format("%s [%s]", element, windowToString(window)));
                receiver.output(element);
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            input.apply(ParDo.of(new LogFn<>()));
            return input;
        }
    }

}
