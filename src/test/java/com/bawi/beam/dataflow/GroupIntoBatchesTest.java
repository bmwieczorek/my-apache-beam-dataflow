package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class GroupIntoBatchesTest {

    private static final Create.TimestampedValues<KV<String, Integer>> TIMESTAMPED_KV_EVENTS = Create.timestamped(
        // W1 [0-5) s
        TimestampedValue.of(KV.of("a", 1), Instant.parse("2021-12-16T00:00:00Z")), // batch 1
        TimestampedValue.of(KV.of("a", 2), Instant.parse("2021-12-16T00:00:01Z")), // batch 1
        TimestampedValue.of(KV.of("b", 3), Instant.parse("2021-12-16T00:00:02Z")), // batch 2

        // W2 [5,10) s
        TimestampedValue.of(KV.of("b", 10), Instant.parse("2021-12-16T00:00:05Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("c", 20), Instant.parse("2021-12-16T00:00:06Z")), // batch 3
        TimestampedValue.of(KV.of("a", 30), Instant.parse("2021-12-16T00:00:07Z")), // batch 4
        TimestampedValue.of(KV.of("b", 40), Instant.parse("2021-12-16T00:00:07Z")), // batch 5
        TimestampedValue.of(KV.of("d", 50), Instant.parse("2021-12-16T00:00:07Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("b", 60), Instant.parse("2021-12-16T00:00:08Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("b", 70), Instant.parse("2021-12-16T00:00:09Z"))  // batch 6 or 7
    );

    @Test
    public void shouldGroupKVsIntoBatches() {
        // given
        Pipeline pipeline = Pipeline.create();

        // when
        PCollection<KV<String, Integer>> windowedElements =
            pipeline.apply(TIMESTAMPED_KV_EVENTS)
                // group per 5 secs window and create kv with key of output2/2021-12-16_00-00 (min granularity)
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollection<KV<String, Iterable<Integer>>> batched =
            windowedElements
                .apply(GroupIntoBatches.ofSize(3))
//                .apply(GroupIntoBatches.ofByteSize(2))
                .apply(ParDo.of(new LogElementWithWindowDetails<>("1")));

        PAssert.that(batched).satisfies((SerializableFunction<Iterable<KV<String, Iterable<Integer>>>, Void>) input -> {
            MatcherAssert.assertThat(input, Matchers.iterableWithSize(7));
            MatcherAssert.assertThat(input, Matchers
                    .either(Matchers.<KV<String, Iterable<Integer>>>hasItem(KV.of("a", List.of(1, 2))))
                    .or(Matchers.hasItem(KV.of("a", List.of(2, 1))))
            );
            MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("b", List.of(3))));
            MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("c", List.of(20))));

            MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("a", List.of(30))));
            MatcherAssert.assertThat(input, Matchers.hasItem(KV.of("d", List.of(50))));
            List<Integer> values = StreamSupport.stream(input.spliterator(), false)
                    .filter(kv -> "b".equals(kv.getKey()))
                    .map((KV<String, Iterable<Integer>> kv) -> {
                        Iterable<Integer> value = kv.getValue();
                        return (int) StreamSupport.stream(value.spliterator(), false).count();
                    }).collect(Collectors.toList());
            MatcherAssert.assertThat("List equality without order",
                    values, containsInAnyOrder(1, 1, 3));
            return null;
        });


        pipeline.run().waitUntilFinish();

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupIntoBatchesTest.class);


    private static class LogElementWithWindowDetails<T> extends DoFn<T, T> {
        private final String label;

        private LogElementWithWindowDetails(String label) {
            this.label = label;
        }

        @ProcessElement
        public void process(@Element T element, OutputReceiver<T> outputReceiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
            String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
            String msg = String.format("[" + label + "] Processing '%s',ts=%s,w=%s,p=%s", element, timestamp, windowString, paneInfo);
            LOGGER.info(msg);
            outputReceiver.output(element);
        }
    }

}
