package com.bawi.beam.dataflow;

import com.bawi.beam.WindowUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyStateAndTimeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyStateAndTimeTest.class);

    static class MyDoFn extends DoFn<KV<String, Integer>, KV<String, Integer>> {
        @StateId("count") private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();
        @StateId("numbers") private final StateSpec<BagState<Integer>> numbersSpec = StateSpecs.bag();

        @TimerId("expiry") private final TimerSpec expiryTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        @ProcessElement
        public void process(@Element KV<String, Integer> kv, BoundedWindow window,
                            @StateId("count") ValueState<Integer> countState,
                            @StateId("numbers") BagState<Integer> numbersState,
                            @TimerId("expiry") Timer expiryTimer) {
            expiryTimer.set(window.maxTimestamp());


            Integer readCount = countState.read();
            int count = firstNonNull(readCount, 0) + 1;
            countState.write(count);
            Iterable<Integer> readNumbers = numbersState.read();
            numbersState.add(kv.getValue());
            if (count >= 2) {
                LOGGER.info("kv={}, w={}, {}->{}, {}->{}", kv.getValue(), WindowUtils.windowToNormalizedString(window), readCount, count, readNumbers, numbersState.read());
                countState.clear();
                numbersState.clear();
            }
        }

        @OnTimer("expiry")
        public void onExpiry(OnTimerContext onTimerContext,
                             @StateId("numbers") BagState<Integer> numbersState) {
//            if (!numbersState.isEmpty().read()) {
                LOGGER.info("Flushing {}", numbersState.read());
                numbersState.clear();
//            }
        }
    }

    public static void main(String[] args) {

        TestStream<KV<String, Integer>> testStream = TestStream
                .create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:00Z"))
                .addElements(TimestampedValue.of(KV.of("a", 10), Instant.parse("2021-12-16T00:00:00Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:01Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(
                        TimestampedValue.of(KV.of("a", 20), Instant.parse("2021-12-16T00:00:01Z")),
                        TimestampedValue.of(KV.of("a", 30), Instant.parse("2021-12-16T00:00:01Z")))

                .advanceProcessingTime(Duration.standardSeconds(1))
                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:02Z"))
                .addElements(TimestampedValue.of(KV.of("a", 40), Instant.parse("2021-12-16T00:00:02Z")))

                .advanceWatermarkTo(Instant.parse("2021-12-16T00:00:03Z"))
                .advanceProcessingTime(Duration.standardSeconds(1))
                .addElements(TimestampedValue.of(KV.of("a", 50), Instant.parse("2021-12-16T00:00:03Z")))

                .advanceWatermarkToInfinity();

        StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(testStream)
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(2))))
                .apply(ParDo.of(new MyDoFn()));
        pipeline.run();
    }

    private static <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }
}
