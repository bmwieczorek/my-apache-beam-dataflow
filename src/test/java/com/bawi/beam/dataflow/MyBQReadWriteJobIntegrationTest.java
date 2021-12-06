package com.bawi.beam.dataflow;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

public class MyBQReadWriteJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBQReadWriteJobIntegrationTest.class);

    @Test
    public void testE2E() throws IOException, InterruptedException {
        // given
        Map<String, String> env = System.getenv();
        LOGGER.info("Environment: {}", env);
        String project = env.get("PROJECT");
        Assert.assertNotNull(project);

        int initialPreLoadedRowCount = 4;
        String query = "select * from " + project + ".bartek_dataset.mysubscription_view";

        // when
        Process bigQueryProcess = runTerraformInfrastructureSetupAsBashProcess("terraform apply -auto-approve -target=module.bigquery");
        logTerraform(bigQueryProcess);
        int bigQueryProcessStatus = bigQueryProcess.waitFor();
        Assert.assertEquals("bigQueryProcess should exit terraform with 0 status code", 0, bigQueryProcessStatus);

        // then
        long totalRows = BigQueryOptions.getDefaultInstance().getService().query(QueryJobConfiguration.of(query)).getTotalRows();
        Assert.assertEquals("Should match initial row count for pre-loaded data", initialPreLoadedRowCount, totalRows);

        Process dataflowTemplateJobProcess = runTerraformInfrastructureSetupAsBashProcess("terraform apply -auto-approve -target=module.dataflow_classic_template_job");
        logTerraform(dataflowTemplateJobProcess);
        int dataflowTemplateJobStatus = dataflowTemplateJobProcess.waitFor();

        Assert.assertEquals("dataflowTemplateJobProcess should exit terraform with 0 status code", 0, dataflowTemplateJobStatus);

        long expectedRowCount = waitUpTo10MinsForDataflowJobToPopulateBiqQuery(query);
        Assert.assertEquals("Dataflow job should create 3 additional rows in BigQuery", (initialPreLoadedRowCount + 3), expectedRowCount);

        LOGGER.info("waiting 150s for job to finish");
        Thread.sleep(150 * 1000);

        Process destroyProcess = runTerraformInfrastructureSetupAsBashProcess("terraform destroy -auto-approve -target=module.bigquery -target=module.dataflow_classic_template_job");
        logTerraform(destroyProcess);
        int destroyStatus = destroyProcess.waitFor();
        Assert.assertEquals("destroyProcess should exit terraform with 0 bigQueryProcessStatus code", 0, destroyStatus);

    }

    private long waitUpTo10MinsForDataflowJobToPopulateBiqQuery(String query) throws InterruptedException {
        long totalRows = 0;

        for (int i = 1; i <= 60; i++) {
            totalRows = BigQueryOptions.getDefaultInstance().getService().query(QueryJobConfiguration.of(query)).getTotalRows();
            LOGGER.info("Returned total rows count: {}", totalRows);
            if (totalRows > 4) {
                break;
            } else {
                LOGGER.info("Waiting for dataflow job to complete and to get expected BigQuery results count ... (attempt {}/100)", i);
                Thread.sleep(10 * 1000L);
            }
        }
        return totalRows;
    }

    private void logTerraform(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                LOGGER.info(line);
            }
        }
    }

    private Process runTerraformInfrastructureSetupAsBashProcess(String cmd) throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/MyBQReadWriteJob"));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File("terraform/MyBQReadWriteJob"));
//        processBuilder.command("./run-terraform.sh");
        //processBuilder.command("bash", "-c", "ls -la");
        processBuilder.command("bash", "-c", cmd);
        return processBuilder.start();
    }

    private String getOutput(Process process) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            return bufferedReader
                    .lines()
                    .collect(Collectors.joining(System.lineSeparator()));
        }
    }
}
