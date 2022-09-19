package com.bawi.beam.dataflow;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
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
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.MyPubsubToGCSJob.ConcatBodyAttrAndMsgIdFn.*;

public class MyPubsubToGCSAvroJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSAvroJobIntegrationTest.class);
    private static final String MY_MSG_BODY = "myMsgBody";
    private static final String MY_MSG_ATTR_VALUE = "myMsgAttrValue";
    private final boolean dataflow_classic_template_enabled = true;
    private final boolean generateMessageDuplicates = true;
    private final boolean isMessageEventTimeIncreasing = false;

    @Before
    @After
    public void cleanUp() throws IOException, InterruptedException {
        Process process = runBashProcess("terraform init && terraform destroy -auto-approve" +
                " -var=\"dataflow_classic_template_enabled=" + dataflow_classic_template_enabled + "\"" +
                " -var=\"dataflow_message_deduplication_enabled=" + generateMessageDuplicates + "\"" +
                " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + !isMessageEventTimeIncreasing + "\""
        );
        logProcess(process);
        int statusCode = process.waitFor();
        Assert.assertEquals("init and destroy process should exit terraform with 0 statusCode code", 0, statusCode);
    }

    @Test
    public void testE2E() throws IOException, InterruptedException, ExecutionException {
        Assert.assertNotNull("Expected Google Project ID to be set as env variable", get("GCP_PROJECT"));
        Assert.assertNotNull("Expected Google Project resource owner to be set as env variable", get("GCP_OWNER"));
        LOGGER.info("Read env variables: GCP_PROJECT={}, GCP_OWNER={}", get("GCP_PROJECT"), get("GCP_OWNER"));

        Process mvnProcess =
                dataflow_classic_template_enabled ?
                runMvnAsBashProcess("mvn clean package -Pdist -DskipTests")
                        :
                runMvnAsBashProcess("mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${GCP_PROJECT} -DskipTests");
        logProcess(mvnProcess);
        int mvnProcessStatus = mvnProcess.waitFor();
        Assert.assertEquals("mvn build should exit with 0 status code", 0, mvnProcessStatus);

        // given
        String topic = get("GCP_OWNER") + "-topic";
        String gcsBucket = get("GCP_PROJECT") + "-" + get("GCP_OWNER") + "-" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase();
        LOGGER.info("topic={}, bucket={}", topic, gcsBucket);
        int numMessages = 10 * 60;

        // when
        Process terraformApplyProcess = runBashProcess("terraform apply -auto-approve" +
                " -var=\"dataflow_classic_template_enabled=" + dataflow_classic_template_enabled + "\"" +
                " -var=\"dataflow_message_deduplication_enabled=" + generateMessageDuplicates + "\"" +
                " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + !isMessageEventTimeIncreasing + "\""
        );

        logProcess(terraformApplyProcess);
        int status = terraformApplyProcess.waitFor();
        Assert.assertEquals("Should exit terraform with 0 status code", 0, status);

//        LOGGER.info("Waiting 2 mins for infra to start");
//        Thread.sleep(120 * 1000);

        long firstEventTimeMillis = System.currentTimeMillis();
        Function<Integer, Long> fn = msgIdx -> isMessageEventTimeIncreasing ? System.currentTimeMillis() : firstEventTimeMillis;

        List<String> messageIds = sendNPubsubMessagesWithDelay(topic, numMessages, Duration.ofMillis(500), fn);

        // then
/*
        List<String> avroRecordsAsStrings = waitUpTo5MinsForDataflowJobToWriteAvroFileToGCSBucket(gcsBucket, numMessages);

        LOGGER.info("Content of avro file(s) read from GCS: {}", avroRecordsAsStrings);

        LOGGER.info("Read {} elements", avroRecordsAsStrings.size());

        Assert.assertEquals(numMessages, avroRecordsAsStrings.size());

//        int messageIndex = 1;
        int messageIndex = numMessages;
        String messageId = messageIds.get(messageIndex - 1);
        String regex = "\\{\"" + BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID + "\": " + "\"" + "body=" + MY_MSG_BODY + messageIndex + ", attributes=\\{" + EVENT_TIME_ATTRIBUTE + "=" + Instant.ofEpochMilli(firstEventTimeMillis) + ", " + MY_MSG_ATTR_NAME + messageIndex + "=" + MY_MSG_ATTR_VALUE + messageIndex + ", " + PUBLISH_TIME_ATTRIBUTE + "=" + "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z" + "}" + ", messageId=" + messageId
                + ", inputDataFreshnessMs=" + "\\d+" + ", customInputDataFreshnessMs=" + "\\d+" + ", customEventTimeInputDataFreshnessMs=" + "\\d+" + "\"}";
//        LOGGER.info("regex={}", regex);

        Pattern pattern = Pattern.compile(regex);

        avroRecordsAsStrings.stream().filter(s -> s.contains("myMsgBody" + messageIndex + ",")).forEach(s -> LOGGER.info("s={}", s));

        Assert.assertTrue(avroRecordsAsStrings.stream().anyMatch(s -> pattern.matcher(s).matches()));

*/
        String query = "select * from " + get("GCP_PROJECT") + "." + get("GCP_OWNER") + "_" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase() + ".my_table";
        int expectedMessageCount = generateMessageDuplicates ? numMessages / 2 : numMessages;
        long actualMessageCount = waitUpTo10MinsForDataflowJobToPopulateBigQuery(query, expectedMessageCount);
        Assert.assertEquals("Expected to get " + expectedMessageCount + " records from BigQuery", expectedMessageCount, actualMessageCount);

        LOGGER.info("Assertions passed, waiting 5 mins before deleting resources");
        Thread.sleep(300 * 1000);
    }

    private long waitUpTo10MinsForDataflowJobToPopulateBigQuery(String query, int expectedRowsCount) throws InterruptedException {
        long totalRows = 0;

        for (int i = 1; i <= 60; i++) {
            totalRows = BigQueryOptions.getDefaultInstance().getService().query(QueryJobConfiguration.of(query)).getTotalRows();
            LOGGER.info("Returned total rows count: {}", totalRows);
            if (totalRows == expectedRowsCount) {
                break;
            } else {
                LOGGER.info("Waiting for dataflow job to complete and to get expected BigQuery results count {} ... (attempt {}/100)", expectedRowsCount, i);
                Thread.sleep(10 * 1000L);
            }
        }
        return totalRows;
    }

    private String get(String variable) {
        return System.getProperty(variable, System.getenv(variable));
    }

    private List<String> sendNPubsubMessagesWithDelay(String topic, int numMessages, Duration sleepDuration, Function<Integer, Long> msgIdxToTimestampMillisFn) throws IOException, InterruptedException, ExecutionException {
        List<String> messageIds = new ArrayList<>();
        for (int i = 1; i <= numMessages; i++) {
            // values of myMsgAttrName attribute are not unique but duplicated: 0, 2, 2, 4, 4 ...
            // attrs: M1: myMsgAttrName1=myMsgAttrValue0, M2: myMsgAttrName2=myMsgAttrValue=2, M3: myMsgAttrName3=myMsgAttrValue=2, M4: myMsgAttrName4=myMsgAttrValue=4
            String myMsgAttrValue = generateMessageDuplicates ? MY_MSG_ATTR_VALUE + (i + 1 - ((i + 1) % 2)) : MY_MSG_ATTR_VALUE + i;

            long eventTimeMillis = msgIdxToTimestampMillisFn.apply(i);
            String messageId = sendMessageToPubsub(MY_MSG_BODY + i, MY_MSG_ATTR_NAME, myMsgAttrValue, eventTimeMillis, topic);

            LOGGER.info("Sent pubsub message ({} of {}), {}, eventTime={}", i, numMessages, myMsgAttrValue, Instant.ofEpochMilli(eventTimeMillis).toDateTime());
            messageIds.add(messageId);
            Thread.sleep(sleepDuration.toMillis());
            if (i % (120) == 0) {
//                Thread.sleep(2 * 60 * 1000); // temporarily pause sending for 2 mins
            }
        }
//        String lastMessageId = sendMessageToPubsub(MY_MSG_BODY + numMessages, MY_MSG_ATTR_NAME + numMessages, MY_MSG_ATTR_VALUE + numMessages, lastEventTimeMillis, topic);
//        LOGGER.info("Sent pubsub message ({} of {}), messageId={}", numMessages, numMessages, lastMessageId);
//        messageIds.add(lastMessageId);
        return messageIds;
    }

    private String sendMessageToPubsub(String myMsgBody, String myMsgAttrName, String myMsgAttrValue, long eventTimeMillis, String topic) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(get("GCP_PROJECT"), topic)).build();
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(myMsgBody))
                .putAllAttributes(ImmutableMap.of(
                        myMsgAttrName, myMsgAttrValue,
                        PUBLISH_TIME_ATTRIBUTE, Instant.now().toString(),
                        EVENT_TIME_ATTRIBUTE, Instant.ofEpochMilli(eventTimeMillis).toString()
                ))
                .build();

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
        return messageId;
    }


    private List<String> waitUpTo5MinsForDataflowJobToWriteAvroFileToGCSBucket(String bucketName, int expectedNumMessages) throws InterruptedException {
        List<String> res = new ArrayList<>();
        Set<String> paths = new TreeSet<>();

        Storage storage = StorageOptions.newBuilder().setProjectId(get("GCP_PROJECT")).build().getService();
        for (int i = 1; i <= 60; i++) {
            Page<Blob> blobs = storage.list(bucketName);
            List<Blob> filteredBlobs = StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                    .filter(blob -> blob.getName().endsWith(".avro"))
//                    .peek(b -> LOGGER.info("filtered blob: {}", b))
                    .collect(Collectors.toList());

            LOGGER.info("Number of blobs ending with .avro: {}", filteredBlobs.size());
            paths = filteredBlobs.stream().map(BlobInfo::getName).collect(Collectors.toCollection(TreeSet::new));

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
                    LOGGER.info("filtered blob paths: {}", paths);
                    return results;
                } else {
                    LOGGER.info("Waiting for dataflow job to write read all messages to GCS ({}/{}) ...(attempt {}/60)", results.size(), expectedNumMessages, i);
                    Thread.sleep(5 * 1000L);
                    res = results;
                }
            } else {
                LOGGER.info("Waiting for dataflow job to start reading {} message(s) from Pubsub, transform and write avro to GCS ... (attempt {}/60)", expectedNumMessages, i);
                Thread.sleep(5 * 1000L);
            }
        }
        LOGGER.info("filtered blob paths: {}", paths);
        return res;
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

    private Process runBashProcess(String cmd) throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/" + MyPubsubToGCSJob.class.getSimpleName()));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File("terraform/" + MyPubsubToGCSJob.class.getSimpleName()));
//        processBuilder.command("./run-terraform.sh");
        //processBuilder.command("bash", "-c", "ls -la");
        processBuilder.command("bash", "-c", cmd);
        return processBuilder.start();
    }

//    private Process runTerraformInfrastructureSetupAsBashProcess(String pathToTerraform) throws IOException {
//        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/" + MyPubsubToGCSJob.class.getSimpleName()));
//        ProcessBuilder processBuilder = new ProcessBuilder();
//        processBuilder.inheritIO();
//        processBuilder.directory(new File(pathToTerraform));
//        processBuilder.command("./run-terraform.sh");
//        //processBuilder.command("bash", "-c", "ls -la");
//        return processBuilder.start();
//    }

//    private String getOutput(Process process) throws IOException {
//        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
//            return bufferedReader
//                    .lines()
//                    .collect(Collectors.joining(System.lineSeparator()));
//        }
//    }
}

/*

bq ls -a -j | head -n1
                                                                   jobId                                                                    Job Type    State      Start Time         Duration
bq ls -a -j | grep '2 Feb' | grep -i load
beam_bq_job_LOAD_mypubsubtogcsjobroot..._00001_00003-0   load       SUCCESS   02 Feb 18:28:40   0:00:02.781000
beam_bq_job_LOAD_mypubsubtogcsjobroot..._00001_00002-0   load       SUCCESS   02 Feb 18:26:14   0:00:04.395000
beam_bq_job_LOAD_mypubsubtogcsjobroot..._00001_00001-0   load       SUCCESS   02 Feb 18:25:10   0:00:03.775000
beam_bq_job_LOAD_mypubsubtogcsjobroot..._00001_00000-0   load       SUCCESS   02 Feb 18:22:11   0:00:05.498000

 */

/*
deduplication on myMsgAttrName and constant even time et:
Job
   .apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription()).withIdAttribute(MY_MSG_ATTR_NAME).withTimestampAttribute(ConcatBodyAttrAndMsgIdFn.EVENT_TIME_ATTRIBUTE))

Test
    private final boolean isMessageAttributeValueUnique = false;
    private final boolean isMessageEventTimeIncreasing = false;

2022-09-18 19:27:22,413 INFO  Sent pubsub message (1 of 600), myMsgAttrValue2, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:23,123 INFO  Sent pubsub message (2 of 600), myMsgAttrValue2, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:24,222 INFO  Sent pubsub message (3 of 600), myMsgAttrValue4, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:24,944 INFO  Sent pubsub message (4 of 600), myMsgAttrValue4, eventTime=2022-09-18T19:27:19.447+02:00
...
2022-09-18 19:41:58,484 INFO  Sent pubsub message (597 of 600), myMsgAttrValue598, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:41:59,143 INFO  Sent pubsub message (598 of 600), myMsgAttrValue598, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:41:59,797 INFO  Sent pubsub message (599 of 600), myMsgAttrValue600, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:42:00,476 INFO  Sent pubsub message (600 of 600), myMsgAttrValue600, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:44:02,120 INFO  Returned total rows count: 300


data freshness increasing constantly to 15mins
system latency up to 3 secs
processed 300 records




41 threads (in main Thread group) with worker harness 8

AfterPubsub_threadId_Thread-26:102	1
AfterPubsub_threadId_Thread-30:118	10
AfterPubsub_threadId_Thread-31:121	5
AfterPubsub_threadId_Thread-33:126	20
AfterPubsub_threadId_Thread-37:141	23
..
AfterPubsub_threadId_Thread-73:812  11


177 threads with worker harness 64

 */
