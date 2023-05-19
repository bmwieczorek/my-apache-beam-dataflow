package com.bawi.beam;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

public class WindowUtils {
    public static String windowToNormalizedString(BoundedWindow window) {
        return window instanceof GlobalWindow ? replace(window.maxTimestamp()) :
                replace(((IntervalWindow) window).start()) + ".." + replace(((IntervalWindow) window).end());
    }

    public static String replace(Instant instant) {
        return instant.toString().replace(":", "_").replace(" ", "_");
    }

}
