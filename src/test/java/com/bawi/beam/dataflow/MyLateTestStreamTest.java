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
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyLateTestStreamTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyLateTestStreamTest.class);

    private static class MyToStringFn<T> extends DoFn<T, String> {
        @ProcessElement
        public void process(@Element T element, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo pane, OutputReceiver<String> outputReceiver) {
            String format = String.format("e=%s,ts=%s,w=%s,pTm=%s", element, timestamp, window, pane.getTiming());
            LOGGER.info(format);
            outputReceiver.output(format);
        }
    }


    TestStream<KV<String, Long>> testStream = TestStream
            .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
            .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:00Z")))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(
                    TimestampedValue.of(KV.of("a", 20L), Instant.parse("2021-12-16T00:00:01Z")),
                    TimestampedValue.of(KV.of("b", 20L), Instant.parse("2021-12-16T00:00:01Z")))

            .advanceProcessingTime(Duration.standardSeconds(1))
            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
            .addElements(TimestampedValue.of(KV.of("b", 30L), Instant.parse("2021-12-16T00:00:02Z")))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(TimestampedValue.of(KV.of("a", 40L), Instant.parse("2021-12-16T00:00:01Z"))) // late

            .advanceWatermarkToInfinity();


    @Test
    public void shouldRejectLateEvent() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply(Sum.longsPerKey())
                .apply(ParDo.of(new MyToStringFn<>()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 x "a" elements + 1 "b" element
                "e=KV{a, 30},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",
                "e=KV{b, 20},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 30},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME"

                // W1 [0,2) element KV{b, 40} is late event so it is discarded
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldAllowLateEventWithinAllowedLatenessDiscardFired() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(2))).withAllowedLateness(Duration.standardSeconds(2)).discardingFiredPanes())
                .apply(Sum.longsPerKey())
//                .apply(Group.globally())
                .apply(ParDo.of(new MyToStringFn<>()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 x "a" elements + 1 "b" element
                "e=KV{a, 30},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",
                "e=KV{b, 20},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 30},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME",

                // W1 [0,2) l is late event, but it is not discarded since allowed lateness, note 40 -> not summed up with previous "a" events
                "e=KV{a, 40},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=LATE"
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldAllowLateEventWithinAllowedLatenessAccumulateFired() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(2))).withAllowedLateness(Duration.standardSeconds(2)).accumulatingFiredPanes())
                .apply(Sum.longsPerKey())
//                .apply(Group.globally())
                .apply(ParDo.of(new MyToStringFn<>()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 x "a" elements + 1 "b" element
                "e=KV{a, 30},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",
                "e=KV{b, 20},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 30},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME",

                // W1 [0,2) KV{a, 40} is late event but it is not discarded since allowed lateness, note 70 -> summed up with all previous "a" events belonging to the same window
                "e=KV{a, 70},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=LATE"
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldDiscardLateEventExceedingAllowedLateness() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);

        TestStream<KV<String, Long>> testStream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(TimestampedValue.of(KV.of("a", 10L), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of(KV.of("a", 20L), Instant.parse("2021-12-16T00:00:01Z")),
                        TimestampedValue.of(KV.of("b", 20L), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("b", 30L), Instant.parse("2021-12-16T00:00:02Z")))

                // advanced by 3s (not 1s) to exceed 2s allowed lateness
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:05Z"))
                .advanceProcessingTime(Duration.standardSeconds(3))
                .addElements(TimestampedValue.of(KV.of("a", 40L), Instant.parse("2021-12-16T00:00:01Z"))) // late
                .advanceWatermarkToInfinity();


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(2))).withAllowedLateness(Duration.standardSeconds(2)).discardingFiredPanes())
                .apply(Sum.longsPerKey())
//                .apply(Group.globally())
                .apply(ParDo.of(new MyToStringFn<>()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 x "a" elements + 1 "b" element
                "e=KV{a, 30},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",
                "e=KV{b, 20},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 30},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME"

                // W1 [0,2) l is late event but discarded since exceeded allowed lateness (discarded regardless discardingFiredPanes or accumulatingFiredPanes)
        );
        pipeline.run().waitUntilFinish();
    }
}
