package com.bawi;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.MyPubsubToConsoleJob.BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID;

public class MyPubsubToGCSAvroJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSAvroJobIntegrationTest.class);

    @Test
    public void testE2E() throws IOException, InterruptedException, ExecutionException {
        String projectId = System.getProperty("PROJECT", System.getenv("PROJECT"));
        Assert.assertNotNull("Expected Google Project ID to be set as env variable", projectId);
        String user = System.getProperty("USER", System.getenv("USER"));
        Assert.assertNotNull("Expected Google Project ID to be set as env variable", user);
        LOGGER.info("Read env variables: PROJECT={}, USER={}", projectId, user);

        // given
        String myMsgBody = "myMsgBody";
        String myMsgAttrName = "myMsgAttrName";
        String myMsgAttrValue = "myMsgAttrValue";

        // when
        Process process = runTerraformInfrastructureSetupAsBashProcess("terraform/MyPubsubToConsoleJob");
        logTerraform(process);
        int status = process.waitFor();
        Assert.assertEquals("Should exit terraform with 0 status code", 0, status);

        String messageId = sendMessageToPubsub(myMsgBody, myMsgAttrName, myMsgAttrValue, user + "-topic", projectId);
        LOGGER.info("Sent pubsub message, messageId={}", messageId);

        // then
        List<String> avroRecordsAsStrings =
                waitUpTo10MinsForDataflowJobToWriteAvroFileToGCSBucket(user + "-mypubsubtoconsolejob", projectId);

        LOGGER.info("Content of avro file(s) read from GCS: {}", avroRecordsAsStrings);

        Assert.assertEquals(1, avroRecordsAsStrings.size());
        Assert.assertEquals("{\"" + BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID + "\": \"" +
                "body=" + myMsgBody + ", " +
                "attributes={" + myMsgAttrName + "=" + myMsgAttrValue + "}, " +
                "messageId=" + messageId+ "\"}", avroRecordsAsStrings.get(0));
    }

    private String sendMessageToPubsub(String myMsgBody, String myMsgAttrName, String myMsgAttrValue, String topic, String projectId) throws IOException, InterruptedException, ExecutionException {
        TopicName topicName = TopicName.of(projectId, topic);
        Publisher publisher = Publisher.newBuilder(topicName).build();
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(myMsgBody))
                .putAllAttributes(ImmutableMap.of(myMsgAttrName, myMsgAttrValue))
                .build();

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
        return messageId;
    }


    private List<String> waitUpTo10MinsForDataflowJobToWriteAvroFileToGCSBucket(String bucketName, String projectId) throws InterruptedException, IOException {
        List<String> results = new ArrayList<>();
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        for (int i = 1; i <= 60; i++) {
            Page<Blob> blobs = storage.list(bucketName);
            Optional<Blob> blobOptional = StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                    .filter(blob -> blob.getName().endsWith(".avro"))
                    .findFirst();

            if (blobOptional.isPresent()) {
                Blob blob = blobOptional.get();
                File tempAvroFile = File.createTempFile("avro-", ".avro");
                blob.downloadTo(Paths.get(tempAvroFile.toURI()));
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tempAvroFile, datumReader);
                while (dataFileReader.hasNext()) {
                    GenericRecord genericRecord = dataFileReader.next();
                    results.add(genericRecord.toString());
                }
                //noinspection ResultOfMethodCallIgnored
                tempAvroFile.delete();
                return results;
            } else {
                LOGGER.info("Waiting for dataflow job to read from pubsub, transform and write avro to GCS ... (attempt {}/100)", i);
                Thread.sleep(10 * 1000L);
            }
        }
        return results;
    }

    private void logTerraform(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                LOGGER.info(line);
            }
        }
    }

    private Process runTerraformInfrastructureSetupAsBashProcess(String pathToTerraform) throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/MyBQReadWriteJob"));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File(pathToTerraform));
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
