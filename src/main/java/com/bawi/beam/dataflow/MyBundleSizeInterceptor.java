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

import java.net.InetAddress;
import java.net.UnknownHostException;

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
        String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
        String msg = String.format("[" + label + "] Processing '%s',ts=%s,w=%s,p=%s", element, timestamp, windowString, paneInfo);
        LOGGER.info(msg);
        receiver.output(element);
    }

    @FinishBundle
    public void finishBundle() {
        bundleSizeDist.update(bundleSize);
        LOGGER.info("[{}][{}] Bundle size is {} for {}", getIP(), getThreadInfo(), bundleSize, label);
    }

    private static String getIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }

    private static String getThreadInfo() {
        return Thread.currentThread().getName() + ":" + Thread.currentThread().getId();
    }
}
