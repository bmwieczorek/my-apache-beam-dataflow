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
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(2)))))
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
