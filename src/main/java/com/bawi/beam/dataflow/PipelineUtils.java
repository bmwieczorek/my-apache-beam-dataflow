package com.bawi.beam.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class PipelineUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineUtils.class);

    public static String[] updateArgsWithDataflowRunner(String[] args, String... additionalArgs) {
        Set<String> merged = new HashSet<>();
        merged.add("--runner=DataflowRunner");
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

        merged.addAll(Arrays.asList(args));
        merged.addAll(Arrays.asList(additionalArgs));
        String[] strings = merged.toArray(String[]::new);
        LOGGER.info("Merged args={}", List.of(merged));
        return strings;
    }

    private static boolean argsMissing(String[] args, String attribute) {
        return Stream.of(args).noneMatch(s -> s.startsWith("--" + attribute + "="));
    }
}
