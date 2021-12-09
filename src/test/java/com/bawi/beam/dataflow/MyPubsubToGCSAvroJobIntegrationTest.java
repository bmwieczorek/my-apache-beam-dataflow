package com.bawi.beam.dataflow;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.MyPubsubToGCSJob.BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID;
import static com.bawi.beam.dataflow.MyPubsubToGCSJob.ConcatBodyAttrAndMsgIdFn.CUSTOM_TIMESTAMP_ATTRIBUTE;

public class MyPubsubToGCSAvroJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSAvroJobIntegrationTest.class);
    public static final String MY_MSG_BODY = "myMsgBody";
    public static final String MY_MSG_ATTR_NAME = "myMsgAttrName";
    public static final String MY_MSG_ATTR_VALUE = "myMsgAttrValue";

    @Test
    public void testE2E() throws IOException, InterruptedException, ExecutionException {
        Assert.assertNotNull("Expected Google Project ID to be set as env variable", get("GCP_PROJECT"));
        Assert.assertNotNull("Expected Google Project resource owner to be set as env variable", get("GCP_OWNER"));
        LOGGER.info("Read env variables: GCP_PROJECT={}, GCP_OWNER={}", get("GCP_PROJECT"), get("GCP_OWNER"));

        Process mvnProcess = runMvnAsBashProcess("mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${GCP_PROJECT} -DskipTests");
        logProcess(mvnProcess);
        int mvnProcessStatus = mvnProcess.waitFor();
        Assert.assertEquals("mvn build should exit with 0 status code", 0, mvnProcessStatus);

        Process terraformInitProcess = runTerraformInfrastructureSetupAsBashProcess("terraform init");
        logProcess(terraformInitProcess);
        int terraformInitProcessStatus = terraformInitProcess.waitFor();
        Assert.assertEquals("terraform init should exit with 0 status code", 0, terraformInitProcessStatus);

        // given
        String topic = get("GCP_OWNER") + "-topic";
        String gcsBucket = get("GCP_PROJECT") + "-" + get("GCP_OWNER") + "-" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase();
        LOGGER.info("topic={}, bucket={}", topic, gcsBucket);
        int numMessages = 10 * 60 * 2;

        // when
        Process terraformApplyProcess = runTerraformInfrastructureSetupAsBashProcess("terraform apply -auto-approve");
        logProcess(terraformApplyProcess);
        int status = terraformApplyProcess.waitFor();
        Assert.assertEquals("Should exit terraform with 0 status code", 0, status);

        Thread.sleep(10 * 1000);

        List<String> messageIds = sendNPubsubMessagesWithDelay(topic, numMessages, Duration.ofMillis(250));

        // then
        List<String> avroRecordsAsStrings = waitUpTo5MinsForDataflowJobToWriteAvroFileToGCSBucket(gcsBucket, numMessages);

        LOGGER.info("Content of avro file(s) read from GCS: {}", avroRecordsAsStrings);

        Assert.assertEquals(numMessages, avroRecordsAsStrings.size());
        String format = String.format("\\{\"%s\": \"body=%s, attributes=\\{customTimestampAttribute=\\d+, %s=%s}, messageId=%s, inputDataFreshnessMs=\\d+, customInputDataFreshnessMs=\\d+\"}",
//                BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID, MY_MSG_BODY, MY_MSG_ATTR_NAME, MY_MSG_ATTR_VALUE + "0", messageIds.get(0));  // deduplication
                BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID, MY_MSG_BODY + "1", MY_MSG_ATTR_NAME + "1", MY_MSG_ATTR_VALUE + "1", messageIds.get(0));
        Pattern pattern = Pattern.compile(format);
        Assert.assertTrue(avroRecordsAsStrings.stream().anyMatch(s -> pattern.matcher(s).matches()));
    }

    @After
    public void cleanUp() throws IOException, InterruptedException {
        Process destroyProcess = runTerraformInfrastructureSetupAsBashProcess("terraform destroy -auto-approve");
        logProcess(destroyProcess);
        int destroyStatus = destroyProcess.waitFor();
        Assert.assertEquals("destroyProcess should exit terraform with 0 destroyStatus code", 0, destroyStatus);
    }


    private String get(String variable) {
        return System.getProperty(variable, System.getenv(variable));
    }

    private List<String> sendNPubsubMessagesWithDelay(String topic, int numMessages, Duration sleepDuration) throws IOException, InterruptedException, ExecutionException {
        List<String> messageIds = new ArrayList<>();
        for (int i = 1; i <= numMessages; i++) {
//            String messageId = sendMessageToPubsub(MY_MSG_BODY + i, MY_MSG_ATTR_NAME, MY_MSG_ATTR_VALUE + (i - (i % 2)), topic); // deduplication
            String messageId = sendMessageToPubsub(MY_MSG_BODY + i, MY_MSG_ATTR_NAME + i, MY_MSG_ATTR_VALUE + i, topic);
            LOGGER.info("Sent pubsub message ({} of {}), messageId={}", i, numMessages, messageId);
            messageIds.add(messageId);
            Thread.sleep(sleepDuration.toMillis());
            if (i % (120 * 2) == 0) {
                Thread.sleep(2 * 60 * 1000);
            }
        }
        return messageIds;
    }

    private String sendMessageToPubsub(String myMsgBody, String myMsgAttrName, String myMsgAttrValue, String topic) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(get("GCP_PROJECT"), topic)).build();
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(myMsgBody))
                .putAllAttributes(ImmutableMap.of(myMsgAttrName, myMsgAttrValue, CUSTOM_TIMESTAMP_ATTRIBUTE, String.valueOf(System.currentTimeMillis())))
                .build();

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
        return messageId;
    }


    private List<String> waitUpTo5MinsForDataflowJobToWriteAvroFileToGCSBucket(String bucketName, int expectedNumMessages) throws InterruptedException {
        Storage storage = StorageOptions.newBuilder().setProjectId(get("GCP_PROJECT")).build().getService();
        for (int i = 1; i <= (120 * 2); i++) {
            Page<Blob> blobs = storage.list(bucketName);
            List<Blob> filteredBlobs = StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                    .filter(blob -> blob.getName().endsWith(".avro"))
//                    .peek(b -> LOGGER.info("filtered blob: {}", b))
                    .collect(Collectors.toList());

            LOGGER.info("filtered blob size: {}", filteredBlobs.size());

            if (filteredBlobs.size() > 0) {
                List<String> results = new ArrayList<>();
                filteredBlobs.forEach(b -> {
                    try {
                        File tempAvroFile = File.createTempFile("avro-", ".avro");
                        b.downloadTo(Paths.get(tempAvroFile.toURI()));
                        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tempAvroFile, datumReader);
                        while (dataFileReader.hasNext()) {
                            GenericRecord genericRecord = dataFileReader.next();
                            results.add(genericRecord.toString());
                        }
                        //noinspection ResultOfMethodCallIgnored
                        tempAvroFile.delete();
                    } catch (IOException e) {
                        LOGGER.error("Problem downloading object from GCS: " + b, e);
                    }
                });
                if (results.size() == expectedNumMessages) {
                    return results;
                } else {
                    LOGGER.info("Waiting for dataflow job to read all messages ({} of {}) from Pubsub, transform and write avro to GCS ... (attempt {}/100)", results.size(), expectedNumMessages, i);
                    Thread.sleep(10 * 1000L);
                }
            } else {
                LOGGER.info("Waiting for dataflow job to start reading {} message(s) from Pubsub, transform and write avro to GCS ... (attempt {}/100)", expectedNumMessages, i);
                Thread.sleep(10 * 1000L);
            }
        }
        return new ArrayList<>();
    }

    private void logProcess(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                LOGGER.info(line);
            }
        }
    }

    private Process runMvnAsBashProcess(String cmd) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.command("bash", "-c", cmd);
        return processBuilder.start();
    }

    private Process runTerraformInfrastructureSetupAsBashProcess(String cmd) throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/MyBQReadWriteJob"));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File("terraform/MyPubsubToGCSJob"));
//        processBuilder.command("./run-terraform.sh");
        //processBuilder.command("bash", "-c", "ls -la");
        processBuilder.command("bash", "-c", cmd);
        return processBuilder.start();
    }

//    private Process runTerraformInfrastructureSetupAsBashProcess(String pathToTerraform) throws IOException {
//        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/MyBQReadWriteJob"));
//        ProcessBuilder processBuilder = new ProcessBuilder();
//        processBuilder.inheritIO();
//        processBuilder.directory(new File(pathToTerraform));
//        processBuilder.command("./run-terraform.sh");
//        //processBuilder.command("bash", "-c", "ls -la");
//        return processBuilder.start();
//    }

    private String getOutput(Process process) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            return bufferedReader
                    .lines()
                    .collect(Collectors.joining(System.lineSeparator()));
        }
    }
}
