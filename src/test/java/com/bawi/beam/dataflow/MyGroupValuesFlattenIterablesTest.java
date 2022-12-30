package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyGroupValuesFlattenIterablesTest {


    @Test
    public void testGroupByKey() {
        Pipeline p = Pipeline.create();
        PCollection<KV<String, Integer>> kIntVals = p
                .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 11)));

        PCollection<KV<String, Iterable<Integer>>> kIterIntVals = kIntVals
                .apply(GroupByKey.create())
                .apply(new Log<>("GroupByKey"));

        PCollection<Iterable<Integer>> iterIntVals = kIterIntVals
                .apply(Values.create())
                .apply(new Log<>("ExtractValsFromKV"));

        PCollection<Integer> result = iterIntVals
                .apply(Flatten.iterables())
                .apply(new Log<>("FlattenValues"));

        p.run().waitUntilFinish();
    }

    static class Log<T> extends PTransform<PCollection<T>, PCollection<T>> {
        private static final Logger LOGGER = LoggerFactory.getLogger(Log.class);
        private final String step;

        Log(String step) {
            this.step = step;
        }

        static class LogFn<E> extends DoFn<E, E> {
            private final String step;

            LogFn(String step) {
                this.step = step;
            }
            @ProcessElement
            public void process(@Element E element, OutputReceiver<E> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
                LOGGER.info("[" + step + "] Processing: {} ", String.format("%s (%s %s)", element, window.getClass().getSimpleName(), window.maxTimestamp()));
                receiver.output(element);
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            input.apply(ParDo.of(new Log.LogFn<>(step)));
            return input;
        }
    }

}
