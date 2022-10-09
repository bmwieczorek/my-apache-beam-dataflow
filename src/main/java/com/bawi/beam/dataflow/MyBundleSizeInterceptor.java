package com.bawi.beam.dataflow;

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class MyBundleSizeInterceptor<T> extends DoFn<T, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBundleSizeInterceptor.class);
    private static final String INTERCEPTOR_CLASS = MyBundleSizeInterceptor.class.getSimpleName();

    private final Distribution bundleSizeDist;
    private final String stepName;
    private int bundleSize;

    public MyBundleSizeInterceptor(String stepName) {
        this.stepName = stepName;
        bundleSizeDist = Metrics.distribution(INTERCEPTOR_CLASS, stepName + "_bundleSize");
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
        LOGGER.info("[{}][{}] Bundle size is {} for {}", getIP(), getThreadInfo(), bundleSize, stepName);
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
