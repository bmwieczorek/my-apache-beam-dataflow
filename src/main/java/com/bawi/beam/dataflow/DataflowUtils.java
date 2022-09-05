package com.bawi.beam.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class DataflowUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataflowUtils.class);

    public static String[] updateDataflowArgs(String[] args, String... additionalArgs) {
        Set<String> merged = new HashSet<>();
        merged.add("--runner=DataflowRunner");
        if (System.getenv("GCP_JAVA_DATAFLOW_RUN_OPTS") != null) {
            merged.addAll(Arrays.asList(System.getenv("GCP_JAVA_DATAFLOW_RUN_OPTS").split(" +")));
        }
        if (System.getenv("GCP_PROJECT") != null) {
            if (!argsContain(args, "projectId")) {
                merged.add("--projectId=" + System.getenv("GCP_PROJECT"));
            }
            if (!argsContain(args, "stagingLocation") && System.getenv("GCP_OWNER") != null) {
                merged.add("--stagingLocation=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/staging");
            }
            if (!argsContain(args, "tempLocation") && System.getenv("GCP_OWNER") != null) {
                merged.add("--tempLocation=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/temp");
            }
        }

        merged.addAll(Arrays.asList(args));
        merged.addAll(Arrays.asList(additionalArgs));
        String[] strings = merged.toArray(String[]::new);
        LOGGER.info("Merged args={}", Arrays.asList(merged));
        return strings;
    }

    private static boolean argsContain(String[] args, String attribute) {
        return Stream.of(args).anyMatch(s -> s.startsWith("--" + attribute + "="));
    }
}
