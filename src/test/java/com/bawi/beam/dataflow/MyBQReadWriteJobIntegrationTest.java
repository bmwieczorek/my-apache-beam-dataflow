package com.bawi.beam.dataflow;

import com.google.cloud.bigquery.BigQuery;
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
import java.util.stream.Collectors;

public class MyBQReadWriteJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBQReadWriteJobIntegrationTest.class);

    @Test
    public void testE2E() throws IOException, InterruptedException {
        // given
        int initialPreLoadedRowCount = 4;
        String query = "select * from bartek_dataset.mysubscription_view";

        // when
        Process process = runTerraformInfrastructureSetupAsBashProcess();
        logTerraform(process);
        int status = process.waitFor();
        Assert.assertEquals("Should exit terraform with 0 status code", 0, status);

        // then
        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        Assert.assertEquals("Should match initial row count for pre-loaded data",
                initialPreLoadedRowCount, bigQuery.query(QueryJobConfiguration.of(query)).getTotalRows());

        long expectedRowCount = waitUpTo10MinsForDataflowJobToPopulateBiqQuery(query, bigQuery);
        Assert.assertEquals("Dataflow job should create 3 additional rows in BigQuery",
                (initialPreLoadedRowCount + 3), expectedRowCount);
    }

    private long waitUpTo10MinsForDataflowJobToPopulateBiqQuery(String q, BigQuery bigQuery) throws InterruptedException {
        long totalRows = 0;
        QueryJobConfiguration queryJobConfiguration = QueryJobConfiguration.of(q);
        for (int i = 1; i <= 60; i++) {
            totalRows = bigQuery.query(queryJobConfiguration).getTotalRows();
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

    private Process runTerraformInfrastructureSetupAsBashProcess() throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/MyBQReadWriteJob"));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File("terraform/MyBQReadWriteJob"));
        processBuilder.command("./run-terraform.sh");
        //processBuilder.command("bash", "-c", "ls -la");
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
