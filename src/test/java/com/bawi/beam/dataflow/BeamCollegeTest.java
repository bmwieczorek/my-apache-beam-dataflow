package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.values.TypeDescriptors.voids;

public class BeamCollegeTest {
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final Logger LOGGER = LoggerFactory.getLogger(BeamCollegeTest.class);

    static class ConcatValueAndTimestampFn extends DoFn<KV<String, Double>, KV<String, String>> {

        @ProcessElement
        public void process(@Element KV<String, Double> element, @Timestamp Instant timestamp,
                                   BoundedWindow window, PaneInfo paneInfo, OutputReceiver<KV<String, String>> out) {
            String ts = FORMATTER.print(timestamp);
            String key = element.getKey();
            String value = String.format("k=%s,v=%s,ts=%s,w=%s,wmts=%s,pane=%s,timing=%s",
                    key, element.getValue(), ts, window.getClass().getSimpleName(), window.maxTimestamp(), paneInfo, paneInfo.getTiming());
            out.output(KV.of(key, value));
        }
    }

    @Test
    public void test() {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(KV.of("A", 1.05), KV.of("A", 1.02), KV.of("A", 1.03)))
                .apply(ParDo.of(new ConcatValueAndTimestampFn()))
                .apply(Combine.globally(new MyToListFn<KV<String, String>>()).withoutDefaults())
                .apply(MapElements.into(voids()).via(input -> {
                    System.out.println(input.toString());
                    input.forEach(kv -> LOGGER.info(kv.toString()));
                    return null;
                }));
        pipeline.run().waitUntilFinish();
    }
}
