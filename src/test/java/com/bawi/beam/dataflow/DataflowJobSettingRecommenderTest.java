package com.bawi.beam.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class DataflowJobSettingRecommenderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataflowJobSettingRecommenderTest.class);
    
    @Test
    public void testExtractFromJobViewAllMatchesExpectedJsonWithAllAssertions() throws Exception {
        String inputPath = "src/test/resources/job_view_all.json";
        String expectedPath = "src/test/resources/job_update_expected.json";

        String input = new String(Files.readAllBytes(Paths.get(inputPath)));
        String expectedJson = new String(Files.readAllBytes(Paths.get(expectedPath)));

        String extractedDetails = DataflowJobSettingRecommender.extractDetailsForUpdateCommand(input);
        LOGGER.info("Extracted details: {}", extractedDetails);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualNode = mapper.readTree(extractedDetails);
        JsonNode expectedNode = mapper.readTree(expectedJson);

        // Top-level assertions
        assertThat(actualNode.get("jobName").asText(), is(expectedNode.get("jobName").asText()));
        assertThat(actualNode.get("update").asBoolean(), is(expectedNode.get("update").asBoolean()));
        assertThat(actualNode.get("jobName").asText(), is("bartek-mypubsubtogcsjob"));
        assertThat(actualNode.get("update").asBoolean(), is(true));

        // Environment assertions
        JsonNode env = actualNode.get("environment");
        JsonNode expectedEnv = expectedNode.get("environment");

        assertThat(env.get("numWorkers").asInt(), is(expectedEnv.get("numWorkers").asInt()));
        assertThat(env.get("maxWorkers").asInt(), is(expectedEnv.get("maxWorkers").asInt()));
        assertThat(env.get("zone").asText(), is(expectedEnv.get("zone").asText()));
        assertThat(env.get("serviceAccountEmail").asText(), is(expectedEnv.get("serviceAccountEmail").asText()));
        assertThat(env.get("tempLocation").asText(), is(expectedEnv.get("tempLocation").asText()));
        assertThat(env.get("bypassTempDirValidation").asBoolean(), is(expectedEnv.get("bypassTempDirValidation").asBoolean()));
        assertThat(env.get("machineType").asText(), is(expectedEnv.get("machineType").asText()));
        assertThat(env.get("subnetwork").asText(), is(expectedEnv.get("subnetwork").asText()));
        assertThat(env.get("ipConfiguration").asText(), is(expectedEnv.get("ipConfiguration").asText()));
        assertThat(env.get("enableStreamingEngine").asBoolean(), is(expectedEnv.get("enableStreamingEngine").asBoolean()));
        assertThat(env.get("diskSizeGb").asInt(), is(expectedEnv.get("diskSizeGb").asInt()));
        assertThat(env.get("network").isNull(), is(expectedEnv.get("network").isNull()));
        assertThat(env.get("numWorkers").asInt(), is(1));
        assertThat(env.get("maxWorkers").asInt(), is(2));
        assertThat(env.get("zone").asText(), is("us-central1-c"));
        assertThat(env.get("serviceAccountEmail").asText(), is("my-service-account-name@my-project-id.iam.gserviceaccount.com"));
        assertThat(env.get("tempLocation").asText(), is("gs://my-project-id-bartek-mypubsubtogcsjob/temp"));
        assertThat(env.get("bypassTempDirValidation").asBoolean(), is(false));
        assertThat(env.get("machineType").asText(), is("n2-highmem-4"));
        assertThat(env.get("subnetwork").asText(), is("https://www.googleapis.com/compute/v1/projects/my-subnetwork-project-id/regions/us-central1/subnetworks/my-subnetwork-id"));
        assertThat(env.get("ipConfiguration").asText(), is("WORKER_IP_PRIVATE"));
        assertThat(env.get("enableStreamingEngine").asBoolean(), is(true));
        assertThat(env.get("diskSizeGb").asInt(), is(200));
        assertThat(env.get("network").isNull(), is(true));

        JsonNode experiments = env.get("additionalExperiments");
        JsonNode expectedExperiments = expectedEnv.get("additionalExperiments");
        assertThat(experiments.size(), is(expectedExperiments.size()));
        for (int i = 0; i < expectedExperiments.size(); i++) {
            assertThat(experiments.get(i).asText(), is(expectedExperiments.get(i).asText()));
        }
        assertThat(experiments.size(), is(3));
        assertThat(experiments.get(0).asText(), is("enable_stackdriver_agent_metrics"));
        assertThat(experiments.get(1).asText(), is("enable_streaming_engine_resource_based_billing"));
        assertThat(experiments.get(2).asText(), is("disable_runner_v2"));

        JsonNode labels = env.get("additionalUserLabels");
        JsonNode expectedLabels = expectedEnv.get("additionalUserLabels");
        assertThat(labels.size(), is(expectedLabels.size()));
        expectedLabels.fieldNames().forEachRemaining(key -> {
            assertThat(labels.has(key), is(true));
            assertThat(labels.get(key).asText(), is(expectedLabels.get(key).asText()));
        });
        assertThat(labels.size(), is(12));
        assertThat(labels.get("additional_experiments3").asText(), is("enable_streaming_engine_resource_based_billing"));
        assertThat(labels.get("additional_experiments1").asText(), is("enable_google_cloud_profiler"));
        assertThat(labels.get("owner").asText(), is("bartek"));
        assertThat(labels.get("dump_heap_on_oom").asText(), is("true"));
        assertThat(labels.get("dataflow_template").asText(), is("classic"));
        assertThat(labels.get("goog-terraform-provisioned").asText(), is("true"));
        assertThat(labels.get("additional_experiments4").asText(), is("disable_runner_v2"));
        assertThat(labels.get("additional_experiments5").asText(), is("disablestringsetmetrics"));
        assertThat(labels.get("additional_experiments0").asText(), is("enable_stackdriver_agent_metrics"));
        assertThat(labels.get("additional_experiments2").asText(), is("enable_google_cloud_heap_sampling"));
        assertThat(labels.get("number_of_worker_harness_threads").asText(), is("0"));
        assertThat(labels.get("enable_streaming_engine").asText(), is("true"));

        // Parameters assertions
        JsonNode params = actualNode.get("parameters");
        JsonNode expectedParams = expectedNode.get("parameters");
        assertThat(params.size(), is(expectedParams.size()));
        expectedParams.fieldNames().forEachRemaining(key -> {
            assertThat(params.has(key), is(true));
            assertThat(params.get(key).asText(), is(expectedParams.get(key).asText()));
        });
        assertThat(params.size(), is(4));
        assertThat(params.get("subscription").asText(), is("projects/my-project-id/subscriptions/bartek-topic-sub"));
        assertThat(params.get("temp").asText(), is("gs://my-project-id-bartek-mypubsubtogcsjob/temp"));
        assertThat(params.get("output").asText(), is("gs://my-project-id-bartek-mypubsubtogcsjob/output"));
        assertThat(params.get("tableSpec").asText(), is("bartek_mypubsubtogcsjob.my_table"));

        // Final strict JSON equality assertion
        assertThat(actualNode, is(expectedNode));
    }

    @Test
    public void testGetRecommendedHighMemMachineType_t2dMachines() {
        // t2d machines should be converted to n2-highmem with 2x vCPUs
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-standard-1"), is("n2-highmem-2"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-standard-2"), is("n2-highmem-4"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-standard-4"), is("n2-highmem-8"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-highcpu-8"), is("n2-highmem-16"));
        // t2d-highmem should be converted to n2-highmem with 2x vCPUs
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-highmem-1"), is("n2-highmem-2"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("t2d-highmem-16"), is("n2-highmem-32"));
    }

    @Test
    public void testGetRecommendedHighMemMachineType_standardMachines() {
        // Standard series should be converted to highmem with 2x vCPUs
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-standard-1"), is("n1-highmem-2"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-standard-4"), is("n1-highmem-8"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2-standard-2"), is("n2-highmem-4"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2d-standard-8"), is("n2d-highmem-16"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("e2-standard-4"), is("e2-highmem-8"));
    }

    @Test
    public void testGetRecommendedHighMemMachineType_highcpuMachines() {
        // Highcpu series should be converted to highmem with 2x vCPUs
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-highcpu-2"), is("n1-highmem-4"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2-highcpu-4"), is("n2-highmem-8"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2d-highcpu-16"), is("n2d-highmem-32"));
    }

    @Test
    public void testGetRecommendedHighMemMachineType_alreadyHighmem() {
        // Already highmem machines should double their vCPUs for higher capacity
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-highmem-2"), is("n1-highmem-4"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2-highmem-4"), is("n2-highmem-8"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n2d-highmem-8"), is("n2d-highmem-16"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-highmem-16"), is("n1-highmem-32"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("e2-highmem-2"), is("e2-highmem-4"));
    }

    @Test
    public void testGetRecommendedHighMemMachineType_edgeCases() {
        // Null input
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType(null), is("manual check required for null"));
        
        // Invalid format (not 3 parts)
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("invalid-format"), is("manual check required for invalid-format"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-standard"), is("manual check required for n1-standard"));
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-standard-4-extra"), is("manual check required for n1-standard-4-extra"));
        
        // Any series (even custom/unknown) is now processed and converted to highmem with 2x vCPUs
        assertThat(DataflowJobSettingRecommender.getRecommendedHighMemMachineType("n1-custom-4"), is("n1-highmem-8"));
    }
}
