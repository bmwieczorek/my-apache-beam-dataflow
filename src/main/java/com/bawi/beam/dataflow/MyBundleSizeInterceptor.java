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

    private final Distribution bundleSizeDist;
    private final String stepName;
    private int bundleSize;

    public MyBundleSizeInterceptor(String stepName) {
        this.stepName = stepName;
        bundleSizeDist = Metrics.distribution(MyBundleSizeInterceptor.class.getSimpleName(), stepName + "_bundleSize");
    }

    @StartBundle
    public void startBundle() {
        bundleSize = 0;
    }

    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver) {
        bundleSize++;
        Metrics.counter(MyBundleSizeInterceptor.class.getSimpleName(), stepName + "_threadId_" + getThread()).inc();
        receiver.output(element);
    }

    @FinishBundle
    public void finishBundle() {
        bundleSizeDist.update(bundleSize);
        LOGGER.info("[{}][{}] Bundle size for {} is {}", getIP(), getThread(), stepName, bundleSize);
    }

    private static String getIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }

    private static String getThread() {
        return Thread.currentThread().getName() + ":" + Thread.currentThread().getId();
    }
}
