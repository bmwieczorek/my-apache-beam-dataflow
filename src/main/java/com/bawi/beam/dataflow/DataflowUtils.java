package com.bawi.beam.dataflow;

import java.util.stream.Stream;

public class DataflowUtils {
    public static String[] updateDataflowArgs(String[] args, String... additionalArgs) {
        return Stream.of(
                   args,
                   System.getenv("GCP_JAVA_DATAFLOW_RUN_OPTS").split(" +"),
                   new String[] {
                       "--runner=DataflowRunner",
                       "--stagingLocation=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/staging"
                   },
                   additionalArgs
               ).flatMap(Stream::of).toArray(String[]::new);
    }
}
