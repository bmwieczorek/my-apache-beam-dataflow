package com.bawi.beam.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.junit.Test;

public class DataflowJobInfoUtilsTest {

    @Test
    public void testGetStreamingEngineEnabled_WhenBooleanFieldIsTrue() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"enableStreamingEngine\": true"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(true));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenBooleanFieldIsFalse() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"enableStreamingEngine\": false"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(false));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenInExperimentsArray() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"experiments\": [\"use_multi_hop_delegation\", \"enable_streaming_engine\", \"disable_runner_v2\"]"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(true));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenNotInExperimentsArray() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"experiments\": [\"use_multi_hop_delegation\", \"disable_runner_v2\"]"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenBothPresent_BooleanTakesPrecedence() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"enableStreamingEngine\": false,"
                + "\"experiments\": [\"enable_streaming_engine\"]"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(false));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenNull() {
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(null);
        
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testGetStreamingEngineEnabled_WhenMissing() {
        String jobDetailsJson = "{"
                + "\"environment\": {"
                + "\"maxWorkers\": 2"
                + "}}";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(nullValue()));
    }

    @Test
    public void testGetStreamingEngineEnabled_WithExpectedJson() throws Exception {
        String jobDetailsJson = """
            {
                "jobName": "my_job_name",
                "environment": {
                    "bypassTempDirValidation": false,
                    "maxWorkers": 3,
                    "numWorkers": 1,
                    "workerRegion": "us-central1",
                    "serviceAccountEmail": "my-email@my-project-id.iam.gserviceaccount.com",
                    "machineType": "n2-highmem-4",
                    "tempLocation": "gs://my-bucket/path/to/temp/location",
                    "subnetwork": "my-subnetwork-http-link",
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    "enableStreamingEngine": true,
                    "additionalExperiments": [
                        "enable_stackdriver_agent_metrics",
                        "enable_streaming_engine_resource_based_billing",
                        "disable_runner_v2"
                    ]
                },
                "parameters": {
                    "outputType": "avro",
                    "tempDir": "gs://my-bucket/path/to/temp/dir",
                    "windowDuration": "1m"
                },
                "update": true
            }
            """;
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(true));
    }

    @Test
    public void testGetStreamingEngineEnabled_InvalidJson() {
        String jobDetailsJson = "invalid json";
        
        Boolean result = DataflowJobDetails.getStreamingEngineEnabledFromDataflowJobDetails(jobDetailsJson);
        
        assertThat(result, is(nullValue()));
    }
}
