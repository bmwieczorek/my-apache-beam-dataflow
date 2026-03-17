package com.bawi.beam.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.bawi.beam.dataflow.DataflowJobDetails.*;
import static com.bawi.beam.dataflow.DataflowJobDetails.getDataflowJobDetails;
import static com.bawi.beam.dataflow.LogUtils.getIp;
import static com.bawi.beam.dataflow.LogUtils.getThreadNameAndId;

public class DataflowJobSettingRecommender {

    /** AI generated code **/

    private static final Logger LOGGER = LoggerFactory.getLogger(DataflowJobSettingRecommender.class);

    private static final String ORIGINAL_JSON_PATH = "original.json";

    // Hardcoded additionalExperiments to always use in the output
    private static final List<String> ADDITIONAL_EXPERIMENTS = Arrays.asList(
            "enable_stackdriver_agent_metrics",
            "enable_streaming_engine_resource_based_billing",
            "disable_runner_v2"
    );

    public static void main(String[] args) throws IOException {
        String originalPath = args.length > 0 ? args[0] : ORIGINAL_JSON_PATH;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode originalRoot = mapper.readTree(new File(originalPath));

        String result = extractDetailsForUpdateCommand(originalRoot.toString());
        System.out.println(result);
    }

    public static String getJobUpdateCurlCommand() {
        String jobDetails = getDataflowJobDetails();
        LOGGER.info("[{}][{}] Job details: {}", getIp(), getThreadNameAndId(), jobDetails);

        String templateLocation = getTemplateLocationFromDataflowJobDetails(jobDetails);
        if ("unknown".equals(templateLocation)) {
            return "Cannot provide curl command to update the job as this is not a templated job";
        }

        Boolean streamingEngineEnabled = getStreamingEngineEnabledFromDataflowJobDetails(jobDetails);
        if (streamingEngineEnabled == null || !streamingEngineEnabled) {
            return "Cannot update running job when the streaming engine is not enabled";
        }

        String extractedJobDetails = "unknown";
        try {
            extractedJobDetails = extractDetailsForUpdateCommand(jobDetails);
        } catch (IOException e) {
            LOGGER.warn("Failed to extract job details", e);
        }

        String launchTemplateUrl = String.format(
                "https://dataflow.googleapis.com/v1b3/projects/%s/locations/%s/templates:launch?gcsPath=%s",
                getProjectId(), getRegion(), templateLocation);

        String jobUpdateCurlCommand = String.format(
                "curl -X POST -H \"Authorization: Bearer $(gcloud auth print-access-token)\" "
                        + "-H \"Content-Type: application/json\" "
                        + "\"%s\""
                        + " -d '%s' ",
                launchTemplateUrl, extractedJobDetails);

        LOGGER.info("[{}][{}] Job update curl command: {}", getIp(), getThreadNameAndId(), jobUpdateCurlCommand);

        return jobUpdateCurlCommand;
    }

    public static String getRecommendedHighMemMachineType(String machineType) {
        if (machineType != null) {
            // Handle full machine type e.g. n1-standard-1, t2d-standard-2, n2d-highcpu-4 etc.
            // Expected format: [family]-[series]-[vcpus]
            // Always return highmem with 4x vCPUs
            String[] parts = machineType.split("-");
            if (parts.length == 3) {
                String family = parts[0];
                String vcpus = parts[2];
                
                // Parse current vCPUs and multiply by 4
                try {
                    int currentVcpus = Integer.parseInt(vcpus);
                    int quadrupledVcpus = currentVcpus * 4;
                    
                    // Special case: if t2d prefix, recommend n2-highmem instead
                    if ("t2d".equals(family)) {
                        return "n2-highmem-" + quadrupledVcpus;
                    }
                    
                    return family + "-highmem-" + quadrupledVcpus;
                } catch (NumberFormatException e) {
                    LOGGER.warn("Unable to parse vCPUs from machine type: {}", machineType, e);
                    return "manual check required for " + machineType;
                }
            }
        }
        return "manual check required for " + machineType;
    }

    public static String extractDetailsForUpdateCommand(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        JsonNode originalRoot = mapper.readTree(json);

        ObjectNode resultNode = mapper.createObjectNode();

        // 1. jobName - from original "name"
        if (originalRoot.has("name")) {
            resultNode.put("jobName", originalRoot.get("name").asText());
        }

        // 2. environment - populated from original input
        ObjectNode resultEnv = mapper.createObjectNode();
        resultNode.set("environment", resultEnv);

        JsonNode origEnv = originalRoot.get("environment");

        if (origEnv != null) {
            JsonNode origSdkOptions = origEnv.get("sdkPipelineOptions");
            JsonNode origOptions = origSdkOptions != null ? origSdkOptions.get("options") : null;

            resultEnv.put("bypassTempDirValidation", false);

            if (origEnv.has("serviceAccountEmail")) {
                resultEnv.put("serviceAccountEmail", origEnv.get("serviceAccountEmail").asText());
            }

            JsonNode workerPools = origEnv.get("workerPools");
            if (workerPools != null && workerPools.isArray() && !workerPools.isEmpty()) {
                JsonNode pool = workerPools.get(0);

                if (pool.has("autoscalingSettings") && pool.get("autoscalingSettings").has("maxNumWorkers")) {
                    resultEnv.put("maxWorkers", pool.get("autoscalingSettings").get("maxNumWorkers").asInt());
                }

                if (pool.has("numWorkers")) {
                    resultEnv.put("numWorkers", pool.get("numWorkers").asInt());
                }

                if (pool.has("machineType")) {
                    String originalMachineType = pool.get("machineType").asText();
                    String recommendedMachineType = getRecommendedHighMemMachineType(originalMachineType);
                    resultEnv.put("machineType", recommendedMachineType);
                }

                if (pool.has("subnetwork")) {
                    resultEnv.put("subnetwork", pool.get("subnetwork").asText());
                }

                if (pool.has("ipConfiguration")) {
                    resultEnv.put("ipConfiguration", pool.get("ipConfiguration").asText());
                }
            }

            // tempLocation from sdkPipelineOptions.options
            if (origOptions != null && origOptions.has("tempLocation")) {
                resultEnv.put("tempLocation", origOptions.get("tempLocation").asText());
            } else if (origOptions != null && origOptions.has("gcpTempLocation")) {
                resultEnv.put("tempLocation", origOptions.get("gcpTempLocation").asText());
            }

            // enableStreamingEngine from sdkPipelineOptions.options
            if (origOptions != null && origOptions.has("enableStreamingEngine")) {
                resultEnv.put("enableStreamingEngine", origOptions.get("enableStreamingEngine").asBoolean());
            }

            // additionalExperiments - always use hardcoded values
            ArrayNode experimentsNode = mapper.createArrayNode();
            for (String experiment : ADDITIONAL_EXPERIMENTS) {
                experimentsNode.add(experiment);
            }
            resultEnv.set("additionalExperiments", experimentsNode);

            // additionalUserLabels - taken directly from original top-level "labels"
            if (originalRoot.has("labels") && !originalRoot.get("labels").isNull()) {
                resultEnv.set("additionalUserLabels", originalRoot.get("labels"));
            }
        }

        // 3. parameters - taken from pipelineDescription.displayData, only items without namespace
        ObjectNode resultParams = mapper.createObjectNode();
        resultNode.set("parameters", resultParams);

        JsonNode pipelineDescription = originalRoot.get("pipelineDescription");
        JsonNode displayData = pipelineDescription != null ? pipelineDescription.get("displayData") : null;

        if (displayData != null && displayData.isArray()) {
            for (JsonNode item : displayData) {
                // Only process items that do NOT have a namespace attribute
                if (!item.has("namespace") && item.has("key")) {
                    String key = item.get("key").asText();

                    // Extract value from type-specific fields (strValue, boolValue, intValue, etc.)
                    String value = null;
                    if (item.has("strValue")) {
                        value = item.get("strValue").asText();
                    } else if (item.has("boolValue")) {
                        value = String.valueOf(item.get("boolValue").asBoolean());
                    } else if (item.has("intValue")) {
                        value = String.valueOf(item.get("intValue").asInt());
                    } else if (item.has("longValue")) {
                        value = String.valueOf(item.get("longValue").asLong());
                    } else if (item.has("floatValue")) {
                        value = String.valueOf(item.get("floatValue").asDouble());
                    } else if (item.has("doubleValue")) {
                        value = String.valueOf(item.get("doubleValue").asDouble());
                    }

                    // Only add non-null values
                    if (value != null) {
                        resultParams.put(key, value);
                    }
                }
            }
        }

        // 4. update: true - always hardcoded
        resultNode.put("update", true);

        return mapper.writeValueAsString(resultNode);
    }
}
