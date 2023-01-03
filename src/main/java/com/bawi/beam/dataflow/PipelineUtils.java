package com.bawi.beam.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineUtils.class);

    public static String[] updateArgsWithDataflowRunner() {
        return updateArgsWithDataflowRunner(new String[]{});
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
}
