package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class MyPipelineWithHashMapSideInputLookupJob {
    private static final String TAG = "sideInputTag";

    public static void main(String[] args) {
        args = PipelineUtils.updateArgsWithDataflowRunner(args
                , "--experiments=enable_stackdriver_agent_metrics"
                                ,"--profilingAgentConfiguration={\"APICurated\":true}"
                                ,"--dataflowServiceOptions=enable_google_cloud_profiler" // adds a link to jobs https://console.cloud.google.com/profiler
        );
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        PCollection<Long> input = pipeline.apply("ReadOrders", GenerateSequence
                .from(0L)
                .withRate(100, Duration.standardSeconds(1))
                .withTimestampFn(i -> Instant.now()));

        PCollectionView<Map<Long, Void>> refMapView = pipeline.apply("ReadRestrictedOrdUuids", GenerateSequence.from(0).to(10_000_000L))
                .apply(MapElements.into(kvs(longs(), voids())).via(e -> KV.of(e, null)))
                .apply(View.asMap());

        // when
        PCollection<Long> filteredAndTransformed = input.apply("FilterOutRestricted",
                ParDo.of(new FilterOutRestrictedOrders()).withSideInput(TAG, refMapView));

        filteredAndTransformed.apply("Write", MyConsoleIO.write());

        PipelineResult result = pipeline.run();
        if ("DirectPipelineResult".equals(result.getClass().getSimpleName())) {
            result.waitUntilFinish(); // usually waitUntilFinish while pipeline development, remove when generating dataflow classic template
        }
    }

    private static class FilterOutRestrictedOrders extends DoFn<Long, Long> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FilterOutRestrictedOrders.class);

        @ProcessElement
        public void process(@Element Long element, @SideInput(TAG) Map<Long, Void> lookup, OutputReceiver<Long> receiver) {
            boolean contains = lookup.containsKey(element);
            LOGGER.info(contains + randomAlphanumeric(200_000));
//            if (lookup.contains(element)) {
//                receiver.output(element);
//            }
            receiver.output(element);
        }
    }
}
