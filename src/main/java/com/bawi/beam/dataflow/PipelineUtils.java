package com.bawi.beam.dataflow;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;

public class PipelineUtils {
    public static final String OWNER = ofNullable(System.getenv("GCP_OWNER")).orElse((System.getenv("user")));

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineUtils.class);

    public static String getJobNameWithOwner(Class<?> clazz) {
        return OWNER + "-" + clazz.getSimpleName().toLowerCase();
    }

    public static String[] updateArgs(String[] args, String... additionalArgs) {
        Set<String> merged = new LinkedHashSet<>();
        merged.addAll(Arrays.asList(args));
        merged.addAll(Arrays.asList(additionalArgs));
        Map<String, String> map = merged.stream().collect(
                Collectors.toMap(s -> s.substring(0, s.indexOf("=")), s -> s.substring(s.indexOf("=") + 1), (s1, s2) -> s1));
        String[] strings = map.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new);
        LOGGER.info("Merged args={}", map);
        return strings;
    }

    public static String[] updateArgsWithDataflowRunner() {
        return updateArgsWithDataflowRunner(new String[]{});
    }

    public static String[] merge(String[] args, String... additionalArgs) {
        return updateArgsWithDataflowRunner(args, additionalArgs);
    }

    public static String[] updateArgsWithDataflowRunner(String[] args, String... additionalArgs) {
        Set<String> merged = new LinkedHashSet<>();
        merged.add("--runner=DataflowRunner");
        merged.addAll(Arrays.asList(args));
        merged.addAll(Arrays.asList(additionalArgs));
        if (System.getenv("GCP_JAVA_DATAFLOW_RUN_OPTS") != null) {
            merged.addAll(Arrays.asList(System.getenv("GCP_JAVA_DATAFLOW_RUN_OPTS").split(" +")));
        }
        if (System.getenv("GCP_PROJECT") != null && System.getenv("GCP_OWNER") != null) {
            if (argsMissing(args, "stagingLocation")) {
                merged.add("--stagingLocation=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/staging");
            }
            if (argsMissing(args, "tempLocation")) {
                merged.add("--tempLocation=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/temp");
            }
        }
        Map<String, String> map = merged.stream().collect(
                Collectors.toMap(s -> s.substring(0, s.indexOf("=")), s -> s.substring(s.indexOf("=") + 1), (s1, s2) -> s1));
        String[] strings = map.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new);
        LOGGER.info("Merged args={}", map);
        return strings;
    }

    private static boolean argsMissing(String[] args, String attribute) {
        return Stream.of(args).noneMatch(s -> s.startsWith("--" + attribute + "="));
    }

    public static boolean isDataflowRunnerOnClasspath() {
        try {
            Class.forName("org.apache.beam.runners.dataflow.DataflowRunner");
            return true;
        } catch (ClassNotFoundException e) {
            // ignore
        }
        return false;
    }

    public static List<String> getDistributions(MetricResults metricResults) {
        return StreamSupport.stream(metricResults.allMetrics().getDistributions().spliterator(), false)
                .map(c -> c.getName().getName() + "=" + c.getAttempted())
                .collect(Collectors.toList());
    }

    public static List<String> getCounters(MetricResults metricResults) {
        return StreamSupport.stream(metricResults.allMetrics().getCounters().spliterator(), false)
                .map(c -> c.getName().getName() + "=" + c.getAttempted())
                .collect(Collectors.toList());
    }

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to get local host name", e);
            return null;
        }
    }

    public static void logMetrics(PipelineResult result) {
        if (result.getClass().getSimpleName().equals("DataflowPipelineJob") || result.getClass().getSimpleName().equals("DirectPipelineResult")) {
            result.waitUntilFinish();
            LOGGER.info("counters={}", getCounters(result.metrics()));
            LOGGER.info("distributions={}", getDistributions(result.metrics()));
        }
    }

}
