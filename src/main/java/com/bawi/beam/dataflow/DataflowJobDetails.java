package com.bawi.beam.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowJobDetails {

    /** AI GENERATED CODE **/

    private static final Logger LOGGER = LoggerFactory.getLogger(DataflowJobDetails.class);

    private DataflowJobDetails() {
        // utility class
    }

    /**
     * Fetches the job details response from the Dataflow API.
     * Returns the raw HTTP response body or null if unsuccessful.
     */
    public static String getDataflowJobDetails() {
        String jobId = getMetadata("instance/attributes/job_id");
        String projectId = getProjectId();
        String region = getRegion();
        if ("unknown".equals(jobId) || "unknown".equals(projectId)) {
            return null;
        }
        try {
            String accessToken = getAccessToken();
            if (accessToken != null) {
                java.net.http.HttpResponse<String> response;
                try (java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient()) {
                    java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                            .uri(java.net.URI.create(String.format("https://dataflow.googleapis.com/v1b3/projects/%s/locations/%s/jobs/%s?view=JOB_VIEW_ALL", projectId, region, jobId)))
                            .header("Authorization", "Bearer " + accessToken)
                            .header("Content-Type", "application/json")
                            .GET()
                            .build();
                    response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
                }
                if (response.statusCode() == 200) {
                    return response.body();
                } else {
                    LOGGER.warn("Failed to get job details via REST: HTTP {} {}", response.statusCode(), response.body());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to get job details from Dataflow API: {}", e.getMessage());
        }
        return null;
    }

    public static String getTemplateLocationFromDataflowJobDetails(String jobDetailsJson) {
        if (jobDetailsJson == null) {
            return "unknown";
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(jobDetailsJson);
            com.fasterxml.jackson.databind.JsonNode env = root.get("environment");
            if (env != null) {
                com.fasterxml.jackson.databind.JsonNode sdkOpts = env.get("sdkPipelineOptions");
                if (sdkOpts != null) {
                    com.fasterxml.jackson.databind.JsonNode options = sdkOpts.get("options");
                    if (options != null && options.has("templateLocation")) {
                        return options.get("templateLocation").asText();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to parse templateLocation from job details: {}", e.getMessage());
        }
        return "unknown";
    }

    public static Boolean getStreamingEngineEnabledFromDataflowJobDetails(String jobDetailsJson) {
        if (jobDetailsJson == null) {
            return null;
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = mapper.readTree(jobDetailsJson);
            com.fasterxml.jackson.databind.JsonNode env = root.get("environment");
            if (env != null) {
                // Check environment.enableStreamingEngine boolean field
                if (env.has("enableStreamingEngine")) {
                    return env.get("enableStreamingEngine").asBoolean();
                }
                // Check environment.experiments array for "enable_streaming_engine" string
                if (env.has("experiments")) {
                    com.fasterxml.jackson.databind.JsonNode experiments = env.get("experiments");
                    if (experiments.isArray()) {
                        for (com.fasterxml.jackson.databind.JsonNode experiment : experiments) {
                            if ("enable_streaming_engine".equals(experiment.asText())) {
                                return true;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to parse enableStreamingEngine from job details: {}", e.getMessage());
        }
        return null;
    }

    public static String getProjectId() {
        return getMetadata("project/project-id");
    }

    public static String getRegion() {
        String zone = getMetadata("instance/zone"); // projects/123/zones/us-central1-a
        if (!"unknown".equals(zone)) {
            String zoneName = zone.substring(zone.lastIndexOf('/') + 1);
            int lastDashIndex = zoneName.lastIndexOf('-');
            if (lastDashIndex > 0) {
                return zoneName.substring(0, lastDashIndex);
            }
        }
        return "us-central1"; // fallback
    }

    public static String getAccessToken() {
        String response = getMetadata("instance/service-accounts/default/token");
        if ("unknown".equals(response)) {
            return null;
        }
        // Parse simple JSON: {"access_token":"...", ...}
        int start = response.indexOf("\"access_token\":\"");
        if (start != -1) {
            start += "\"access_token\":\"".length();
            int end = response.indexOf("\"", start);
            if (end != -1) {
                return response.substring(start, end);
            }
        }
        return null;
    }

    public static String getMachineType() {
        String fullyQualifiedMachineType = getMetadata("instance/machine-type");
        if ("unknown".equals(fullyQualifiedMachineType)) {
            return "unknown";
        }
        return fullyQualifiedMachineType.substring(fullyQualifiedMachineType.lastIndexOf('/') + 1);
    }

    private static String getMetadata(String path) {
        try {
            java.net.URL url = java.net.URI.create("http://metadata.google.internal/computeMetadata/v1/" + path).toURL();
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Metadata-Flavor", "Google");
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
                return reader.readLine();
            }
        } catch (Exception e) {
            return "unknown";
        }
    }

}

