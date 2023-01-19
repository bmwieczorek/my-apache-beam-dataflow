package com.bawi.beam.dataflow;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;

public class LogUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUtils.class);

    public static String ipAddressAndThread() {
//        InetAddress localHostAddress = getLocalHostAddress();
        Thread thread = Thread.currentThread();
//        String total = format(Runtime.getRuntime().totalMemory());
//        String free = format(Runtime.getRuntime().freeMemory());
//        String used = format(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
//        String max = format(Runtime.getRuntime().maxMemory());
//        return String.format("%s|i:%s|n:%s|g:%s|c:%s|u:%s|f:%s|t:%s|m:%s",
//                localHostAddress, thread.getId(), thread.getName(), thread.getThreadGroup().getName(), Runtime.getRuntime().availableProcessors(), used, free, total, max);

        return String.format("%s|i:%s|n:%s",
                getLocalHostAddress(), thread.getId(), thread.getName());
    }

    public static String getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host address", e);
            return null;
        }
    }

    public static String getLocalHostAddressSpaced() {
        String localHostAddress = getLocalHostAddress();
        return localHostAddress == null ? "" : localHostAddress.replace(".","_");
    }

    public static String format(long value) {
        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setGroupingUsed(true);
        return numberFormat.format(value);
    }

    public static String windowToString(BoundedWindow window) {
        return window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
    }
}
