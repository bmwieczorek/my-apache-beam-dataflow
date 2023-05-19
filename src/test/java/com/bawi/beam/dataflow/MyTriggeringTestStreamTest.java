package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTriggeringTestStreamTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyTriggeringTestStreamTest.class);

    private static class MyToStringFn<T> extends DoFn<T, String> {
        @ProcessElement
        public void process(@Element T element, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo pane, OutputReceiver<String> outputReceiver) {
            String format = String.format("e=%s,ts=%s,w=%s,pTm=%s", element, timestamp, window, pane.getTiming());
            LOGGER.info(format);
            outputReceiver.output(format);
        }
    }

    @Test
    public void shouldTriggerAfterTimeProcessingWithoutDelay() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(
                        TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00Z")))
                .advanceProcessingTime(Duration.millis(500))

                .addElements(
                        TimestampedValue.of(KV.of("a", 5L), Instant.parse("2021-12-16T00:00:00.500Z")))
                .advanceProcessingTime(Duration.millis(500))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window
                        .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .withAllowedLateness(Duration.standardSeconds(2))
                        .withTimestampCombiner(TimestampCombiner.LATEST)
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(100), // HERE NEVER REACHED
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(0)) // NO DELAY
                        )))
                        .discardingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 5},ts=2021-12-16T00:00:00.500Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 10},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 100},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        IntervalWindow window = new IntervalWindow(Instant.parse("2021-12-16T00:00:00.000Z"), Duration.standardSeconds(5));
        PAssert.that(pCollection).inWindow(window).containsInAnyOrder(
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 5},ts=2021-12-16T00:00:00.500Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 10},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 100},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );

        PAssert.that(pCollection).inEarlyPane(window).containsInAnyOrder(
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 5},ts=2021-12-16T00:00:00.500Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 10},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 100},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY"
        );
        PAssert.that(pCollection).inFinalPane(window).containsInAnyOrder(
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldTriggerAfterTimeProcessingWith1SecondDelay0() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(
                        TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window
                        .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .withAllowedLateness(Duration.standardSeconds(2))
                        .withTimestampCombiner(TimestampCombiner.LATEST)
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(100), // HERE NEVER REACHED
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)) // 1 SEC DELAY
                        )))
                        .discardingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                "e=KV{a, 13},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        IntervalWindow window = new IntervalWindow(Instant.parse("2021-12-16T00:00:00.000Z"), Duration.standardSeconds(5));
        PAssert.that(pCollection).inWindow(window).containsInAnyOrder(
                "e=KV{a, 13},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );

        PAssert.that(pCollection).inEarlyPane(window).containsInAnyOrder(
                "e=KV{a, 13},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY"
        );
        PAssert.that(pCollection).inFinalPane(window).containsInAnyOrder(
                "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        pipeline.run().waitUntilFinish();
    }


@Test
public void shouldTriggerAfterTimeProcessingWith1SecondDelay() {
// given
StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
options.setStreaming(true);
Pipeline pipeline = Pipeline.create(options);

TestStream<KV<String, Long>> testStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
    .addElements(
            TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")), // first element in first pane, start 1 min timer
            TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00.001Z"))) // first pane

    .advanceProcessingTime(Duration.millis(500)) // still within 1 sec waiting delay
    .addElements(
            TimestampedValue.of(KV.of("a", 4L), Instant.parse("2021-12-16T00:00:00.500Z"))) // first pane

    .advanceProcessingTime(Duration.millis(500)) // still within 1 sec waiting delay
    .addElements(
            TimestampedValue.of(KV.of("a", 8L), Instant.parse("2021-12-16T00:00:01Z"))) // fourth element in first pane
    // end of first pane

    // second pane  as above 1 sec waiting delay
    .advanceProcessingTime(Duration.millis(100)) // 100ms after first pane timer finished, first element in second pane, start 1 min countdown
    .addElements(
            TimestampedValue.of(KV.of("a", 9L), Instant.parse("2021-12-16T00:00:01.100Z")))

    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01.100Z"))
    .advanceProcessingTime(Duration.standardSeconds(1))
    .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z"))) // within 1 sec timer
    // end of second pane

    // third pane
    .advanceProcessingTime(Duration.standardSeconds(1))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
    .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

    .advanceProcessingTime(Duration.standardSeconds(1))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
    .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))
    // end of third pane

     // 4th pane
    .advanceProcessingTime(Duration.millis(1500))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:04.500Z"))
    .addElements(TimestampedValue.of(KV.of("a", 2000L), Instant.parse("2021-12-16T00:00:04.500Z"))) // closed by watermark pass end of window ON_TIME

    // second window
    // 5th pane
    .advanceProcessingTime(Duration.millis(1000))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:05.500Z"))
    .addElements(TimestampedValue.of(KV.of("a", 10000L), Instant.parse("2021-12-16T00:00:05.500Z")))


    .advanceProcessingTime(Duration.millis(1500))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:07Z"))
    .addElements(TimestampedValue.of(KV.of("a", 20000L), Instant.parse("2021-12-16T00:00:07Z"))) // closed by watermark pass end of window (infinity)

    .addElements(TimestampedValue.of(KV.of("a", 5000L), Instant.parse("2021-12-16T00:00:04.900Z"))) // LATE

    .advanceWatermarkToInfinity();


// when
PCollection<String> pCollection = pipeline.apply(testStream)
    .apply(Window
            .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
            .withAllowedLateness(Duration.standardSeconds(3))
            .withTimestampCombiner(TimestampCombiner.LATEST) // timestamp of last grouped message will be used as timestamp for new group elements
            .triggering(Repeatedly.forever(AfterFirst.of(
                    AfterPane.elementCountAtLeast(100), // BIG ENOUGH COUNT TO BE NEVER REACHED
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)) // SHORT ENOUGH DELAY TO BE ALWAYS EXECUTED
            )))
            .discardingFiredPanes())
    .apply(Sum.longsPerKey())
    .apply(ParDo.of(new MyToStringFn<>()));

// then
PAssert.that(pCollection).containsInAnyOrder(
        // w1=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z)
        "e=KV{a, 15},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
        "e=KV{a, 19},ts=2021-12-16T00:00:01.100Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
        "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
        "e=KV{a, 2000},ts=2021-12-16T00:00:04.500Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME",

        // w2=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z)
        "e=KV{a, 10000},ts=2021-12-16T00:00:05.500Z,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z),pTm=EARLY",
        "e=KV{a, 20000},ts=2021-12-16T00:00:07.000Z,w=[2021-12-16T00:00:05.000Z..2021-12-16T00:00:10.000Z),pTm=ON_TIME",

        // w1=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z)
        "e=KV{a, 5000},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=LATE"
);

pipeline.run().waitUntilFinish();
}

@Test
public void shouldTriggerAfterElementsCount() {
// given
StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
options.setStreaming(true);
Pipeline pipeline = Pipeline.create(options);

TestStream<KV<String, Long>> testStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
    .addElements(
            TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")), // taken by first pane
            TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00.001Z"))) // first pane

    .advanceProcessingTime(Duration.millis(500)) // still below 10 sec delay
    .addElements(
            TimestampedValue.of(KV.of("a", 4L), Instant.parse("2021-12-16T00:00:00.500Z"))) // first pane

    .advanceProcessingTime(Duration.millis(500)) //  still below 10 sec delay
    .addElements(
            TimestampedValue.of(KV.of("a", 8L), Instant.parse("2021-12-16T00:00:01Z"))) // fourth element in first pane
    // end of first pane

    // second pane still below 10 sec delay
    .advanceProcessingTime(Duration.millis(100)) // still below 10 sec delay
    .addElements(
            TimestampedValue.of(KV.of("a", 9L), Instant.parse("2021-12-16T00:00:01.100Z")))

    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01.100Z"))
    .advanceProcessingTime(Duration.standardSeconds(1))
    .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))  // still below 10 sec delay
    // end of second pane

    // third pane
    .advanceProcessingTime(Duration.standardSeconds(1))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
    .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

    .advanceProcessingTime(Duration.standardSeconds(1))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
    .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))
    // end of third pane

    // last pane (not triggered by count (here only 1 element) and not triggered by processing time (delay 10s)
    .advanceProcessingTime(Duration.standardSeconds(1))
    .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:04Z"))
    .addElements(TimestampedValue.of(KV.of("a", 10000L), Instant.parse("2021-12-16T00:00:04Z")))

    .advanceWatermarkToInfinity();


    // when
    PCollection<String> pCollection = pipeline.apply(testStream)
        .apply(Window
                .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                .withAllowedLateness(Duration.standardSeconds(2))
                .withTimestampCombiner(TimestampCombiner.LATEST) // timestamp of last grouped message will be used as timestamp for new group elements
                .triggering(Repeatedly.forever(AfterFirst.of(
                        AfterPane.elementCountAtLeast(2), // SMALL COUNT TO BE ALWAYS EXECUTED
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)) // LONG ENOUGH DELAY TO BE NEVER EXECUTED
                )))
                .discardingFiredPanes())
        .apply(Sum.longsPerKey())
        .apply(ParDo.of(new MyToStringFn<>()));

    // then
    PAssert.that(pCollection).containsInAnyOrder(
            "e=KV{a, 3},ts=2021-12-16T00:00:00.001Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 12},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 19},ts=2021-12-16T00:00:01.100Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 10000},ts=2021-12-16T00:00:04.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"

    );
    IntervalWindow window = new IntervalWindow(Instant.parse("2021-12-16T00:00:00.000Z"), Duration.standardSeconds(5));

    PAssert.that(pCollection).inEarlyPane(window).containsInAnyOrder(
            "e=KV{a, 3},ts=2021-12-16T00:00:00.001Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 12},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 19},ts=2021-12-16T00:00:01.100Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
            "e=KV{a, 1100},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY"

    );

    PAssert.that(pCollection).inFinalPane(window).containsInAnyOrder(
            "e=KV{a, 10000},ts=2021-12-16T00:00:04.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME");

    pipeline.run().waitUntilFinish();
}

    @Test
    public void shouldTriggerAfterTimeProcessingWith2SecondsDelay() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(
                        TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window
                        .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .withAllowedLateness(Duration.standardSeconds(2))
                        .withTimestampCombiner(TimestampCombiner.LATEST)
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(100), // BIG ENOUGH COUNT TO BE NEVER REACHED
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2)) // SHORT ENOUGH DELAY TO BE ALWAYS EXECUTED
                        )))
                        .discardingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                "e=KV{a, 113},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        IntervalWindow window = new IntervalWindow(Instant.parse("2021-12-16T00:00:00.000Z"), Duration.standardSeconds(5));
        PAssert.that(pCollection).inWindow(window).containsInAnyOrder(
                "e=KV{a, 113},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );

        PAssert.that(pCollection).inEarlyPane(window).containsInAnyOrder(
                "e=KV{a, 113},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY"
        );
        PAssert.that(pCollection).inFinalPane(window).containsInAnyOrder(
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldTriggerElementCountAtLeast() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(
                        TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
         // default WindowingStrategy: TimestampCombiner.END_OF_WINDOW, AccumulationMode.DISCARDING_FIRED_PANES, DEFAULT_ALLOWED_LATENESS = Duration.ZERO
                .apply(Window
                        .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .withAllowedLateness(Duration.standardSeconds(2))
                        // combiner defines the timestamp of new element which is the result of grouping:
                        // end of window, or in relation to timestamp of elements being grouped
//                        .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
//                        .withTimestampCombiner(TimestampCombiner.EARLIEST)
                        .withTimestampCombiner(TimestampCombiner.LATEST)
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(2),
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2))
                        )))
                        .discardingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,5) emit immediately first 2 "a" elements grouped
//                "e=KV{a, 3},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY", // EARLY
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY", // EARLY

                // W1 [0,5) remaining 2 "a" element
//                "e=KV{a, 110},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
//                "e=KV{a, 110},ts=2021-12-16T00:00:01.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 110},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",

                // W2 [0,5)
//                "e=KV{a, 1000},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        IntervalWindow window = new IntervalWindow(Instant.parse("2021-12-16T00:00:00.000Z"), Duration.standardSeconds(5));
        PAssert.that(pCollection).inWindow(window).containsInAnyOrder(
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 110},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        PAssert.that(pCollection).inEarlyPane(window).containsInAnyOrder(
                "e=KV{a, 3},ts=2021-12-16T00:00:00.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY",
                "e=KV{a, 110},ts=2021-12-16T00:00:02.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY"
        );
        PAssert.that(pCollection).inFinalPane(window).containsInAnyOrder(
                "e=KV{a, 1000},ts=2021-12-16T00:00:03.000Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldTriggerPastFirstElementInPanePlusDelay() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(
                        TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:00Z")),
                        TimestampedValue.of(KV.of("a", 2L), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 100L), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .addElements(TimestampedValue.of(KV.of("a", 1000L), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();

        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window
                        .<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(5)))
                        .withAllowedLateness(Duration.standardSeconds(0))
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(10),
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2)))))
                        .discardingFiredPanes())
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));

        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,5) emit immediately after 2 secs from first 2 "a" elements arrived
                "e=KV{a, 113},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=EARLY", // EARLY

                // W2 [0,5)
                "e=KV{a, 1000},ts=2021-12-16T00:00:04.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:05.000Z),pTm=ON_TIME"
        );
        pipeline.run().waitUntilFinish();
    }
}
