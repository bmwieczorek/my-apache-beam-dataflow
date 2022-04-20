package com.bawi.beam.dataflow;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyBundleSizeInterceptor<T> extends DoFn<T, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBundleSizeInterceptor.class);

    private final Distribution bundleSizeDist;
    private final String name;
    private Integer bundleSize;

    public MyBundleSizeInterceptor() {
        this(MyBundleSizeInterceptor.class.getSimpleName());
    }

    public MyBundleSizeInterceptor(String name) {
        this.name = name;
        bundleSizeDist = Metrics.distribution(MyBundleSizeInterceptor.class.getSimpleName(), name + "_bundleSize");
    }

    @StartBundle
        public void startBundle() {
            bundleSize = 0;
        }

        @ProcessElement
        public void process(@Element T element, OutputReceiver<T> receiver) {
            bundleSize++;
            receiver.output(element);
        }

        @FinishBundle
        public void finishBundle() {
            bundleSizeDist.update(bundleSize);
            LOGGER.info("Bundle size for " + name + " is {}", bundleSize);
        }
}
