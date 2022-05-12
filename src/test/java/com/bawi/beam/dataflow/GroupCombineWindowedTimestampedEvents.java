package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class GroupCombineWindowedTimestampedEvents {
    

    private static class MyStringFormatFn<T> extends DoFn<T, String> {
        @ProcessElement
        public void process(@Element T element, @Timestamp Instant timestamp, BoundedWindow window, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(String.format("e=%s,ts=%s,wMaxTs=%s,wCl=%s,w=%s", element, timestamp, window.maxTimestamp(), window.getClass().getSimpleName(), window));
        }
    }

    Create.TimestampedValues<KV<String, Integer>> timestampedKVEvents = Create.timestamped(
        // W1 [0-5) s
        TimestampedValue.of(KV.of("a", 1), Instant.parse("2021-12-16T00:00:00Z")),
        TimestampedValue.of(KV.of("a", 2), Instant.parse("2021-12-16T00:00:01Z")),

        // W2 [5,10) s
        TimestampedValue.of(KV.of("b", 10), Instant.parse("2021-12-16T00:00:05Z")),
        TimestampedValue.of(KV.of("c", 20), Instant.parse("2021-12-16T00:00:06Z")),
        TimestampedValue.of(KV.of("a", 30), Instant.parse("2021-12-16T00:00:07Z"))
    );

    @Test
    public void shouldCountPerKey() {
        // given
        Pipeline pipeline = Pipeline.create();

        // when
        PCollection<String> pCollection = pipeline.apply(timestampedKVEvents)
            // group per 5 secs window and count how many elements per key in each window
            .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(5))).withTimestampCombiner(TimestampCombiner.LATEST)) // timestamp combiner impacts what will be the grouped event timestamp in relation to ts of grouped events
            .apply(Count.perKey())
            .apply(ParDo.of(new MyStringFormatFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0-5) s
                "e=KV{a, 2},ts=2021-12-16T00:00:01.000Z,wMaxTs=2021-12-16T00:00:04.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z)", // 2 x "a" grouped as belonging to same window 

                // W2 [5,10) s
                "e=KV{b, 1},ts=2021-12-16T00:00:05.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)",
                "e=KV{c, 1},ts=2021-12-16T00:00:06.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)",
                "e=KV{a, 1},ts=2021-12-16T00:00:07.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)"  // latest "a" belongs to next window
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldSumPerKey() {
        // given
        Pipeline pipeline = Pipeline.create();

        // when
        PCollection<String> pCollection = pipeline.apply(timestampedKVEvents)
                // group per 5 secs window and sum elements value per key in each window
                .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(5))).withTimestampCombiner(TimestampCombiner.LATEST)) // timestamp combiner impacts what will be the grouped event timestamp in relation to ts of grouped events
                .apply(Sum.integersPerKey())
                .apply(ParDo.of(new MyStringFormatFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0-5) s
                "e=KV{a, 3},ts=2021-12-16T00:00:01.000Z,wMaxTs=2021-12-16T00:00:04.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z)", // 2 x "a" grouped as belonging to same window

                // W2 [5,10) s
                "e=KV{b, 10},ts=2021-12-16T00:00:05.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)",
                "e=KV{c, 20},ts=2021-12-16T00:00:06.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)",
                "e=KV{a, 30},ts=2021-12-16T00:00:07.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)"  // latest "a" belongs to next window
        );
        pipeline.run().waitUntilFinish();
    }

    Create.TimestampedValues<String> timestampedEvents = Create.timestamped(
            // W1 [0-5) s
            TimestampedValue.of("a1", Instant.parse("2021-12-16T00:00:00Z")),
            TimestampedValue.of("a2", Instant.parse("2021-12-16T00:00:01Z")),

            // W2 [5,10) s
            TimestampedValue.of("b1", Instant.parse("2021-12-16T00:00:05Z")),
            TimestampedValue.of("c1", Instant.parse("2021-12-16T00:00:06Z")),
            TimestampedValue.of("a3", Instant.parse("2021-12-16T00:00:07Z"))
    );

    @Test
    public void shouldGroupCombineGlobally() {
        // given
        Pipeline pipeline = Pipeline.create();

        // when
        PCollection<String> pCollection = pipeline.apply(timestampedEvents)
                // group per 5 secs window and sum elements value per key in each window
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(5))).withTimestampCombiner(TimestampCombiner.LATEST)) // timestamp combiner impacts what will be the grouped event timestamp in relation to ts of grouped events
//                .apply(Group.globally()) // same as below but random order of sub-elements in result element of type iterable
                .apply(Combine.globally(new MyToTreeSetFn<String>()).withoutDefaults())
                .apply(ParDo.of(new MyStringFormatFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0-5) s
                "e=[a1, a2],ts=2021-12-16T00:00:01.000Z,wMaxTs=2021-12-16T00:00:04.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z)",

                // W2 [5,10) s
                "e=[a3, b1, c1],ts=2021-12-16T00:00:07.000Z,wMaxTs=2021-12-16T00:00:09.999Z,wCl=IntervalWindow,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)"
        );
        pipeline.run().waitUntilFinish();
    }
}
