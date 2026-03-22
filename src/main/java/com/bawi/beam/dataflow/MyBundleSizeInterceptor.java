package com.bawi.beam.dataflow;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.bawi.beam.dataflow.LogUtils.getIpThreadNameAndThreadId;

public class MyBundleSizeInterceptor<T> extends DoFn<T, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBundleSizeInterceptor.class);
    private static final String INTERCEPTOR_CLASS = MyBundleSizeInterceptor.class.getSimpleName();

    private final Distribution bundleSizeDist;
    private final String label;
    private int bundleSize;

    public MyBundleSizeInterceptor(String label) {
        this.label = label;
        bundleSizeDist = Metrics.distribution(INTERCEPTOR_CLASS, label + "_bundleSize");
    }

    @StartBundle
    public void startBundle() {
        bundleSize = 0;
    }

    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
        bundleSize++;
        LOGGER.info("[{}] {} Process bundleSize={},ts={},w={},p={}", getIpThreadNameAndThreadId(), label, bundleSize, timestamp, getWindowInfo(window), paneInfo);
        receiver.output(element);
    }

    @FinishBundle
    public void finishBundle() {
        bundleSizeDist.update(bundleSize);
        LOGGER.info("[{}][{}] Finish bundleSize={}", getIpThreadNameAndThreadId(), label, bundleSize);
    }

    private static String getWindowInfo(BoundedWindow window) {
        return window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
    }
}
