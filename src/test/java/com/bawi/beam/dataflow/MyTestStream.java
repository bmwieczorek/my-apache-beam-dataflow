package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
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

public class MyTestStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyTestStream.class);

    private static class MyToStringFn extends DoFn<KV<String, Long>, String> {
        @ProcessElement
        public void process(@Element KV<String, Long> element, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo pane, OutputReceiver<String> outputReceiver) {
            String format = String.format("e=%s,ts=%s,w=%s,pTm=%s", element, timestamp, window, pane.getTiming());
            LOGGER.info(format);
            outputReceiver.output(format);
        }
    }


    TestStream<KV<String, Long>> testStream = TestStream
            .create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
            .addElements(TimestampedValue.of(KV.of("a", 0L), Instant.parse("2021-12-16T00:00:00Z")))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(TimestampedValue.of(KV.of("a", 1L), Instant.parse("2021-12-16T00:00:01Z")), TimestampedValue.of(KV.of("b", 1L), Instant.parse("2021-12-16T00:00:01")))

            .advanceProcessingTime(Duration.standardSeconds(1))
            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
            .addElements(TimestampedValue.of(KV.of("b", 2L), Instant.parse("2021-12-16T00:00:02Z")))

            .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
            .advanceProcessingTime(Duration.standardSeconds(1))
            .addElements(TimestampedValue.of(KV.of("c", 3L), Instant.parse("2021-12-16T00:00:01Z"))) // late
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
                .apply(Count.perKey())
                .apply(ParDo.of(new MyToStringFn()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 "a" elements
                "e=KV{a, 2},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 1},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME"

                // W1 [0,2) c is late event so it is discarded
        );
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldAllowLateEvent() {
        // given
        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);


        // when
        PCollection<String> pCollection = pipeline.apply(testStream)
                .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(2))).withAllowedLateness(Duration.standardSeconds(2)).discardingFiredPanes())

// here it does not matter if it is discarding or accumulating
//                .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardSeconds(2))).withAllowedLateness(Duration.standardSeconds(2)).accumulatingFiredPanes())
                .apply(Count.perKey())
                .apply(ParDo.of(new MyToStringFn()));


        // then
        PAssert.that(pCollection).containsInAnyOrder(
                // W1 [0,2) 2 "a" elements
                "e=KV{a, 2},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=ON_TIME",

                // W2 [2,4)
                "e=KV{b, 1},ts=2021-12-16T00:00:03.999Z,w=[2021-12-16T00:00:02.000Z..2021-12-16T00:00:04.000Z),pTm=ON_TIME",

                // W1 [0,2) c is late event but it is not discarded since allowed lateness
                "e=KV{c, 1},ts=2021-12-16T00:00:01.999Z,w=[2021-12-16T00:00:00.000Z..2021-12-16T00:00:02.000Z),pTm=LATE"
        );
        pipeline.run().waitUntilFinish();
    }
}
