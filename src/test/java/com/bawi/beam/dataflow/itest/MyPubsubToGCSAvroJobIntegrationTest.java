package com.bawi.beam.dataflow.itest;

import com.bawi.beam.dataflow.MyPubsubToGCSJob;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.MyPubsubToGCSJob.*;

public class MyPubsubToGCSAvroJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSAvroJobIntegrationTest.class);
    private static final String MY_MSG_BODY = "myMsgBody";
    private static final String MY_MSG_ATTR_VALUE = "myMsgAttrValue";
    private final boolean dataflow_classic_template_enabled = true;
    private final boolean generateMessageDuplicates = false;
    private final boolean isMessageEventTimeIncreasing = true;
    private final boolean skip_wait_on_job_termination = false;
    private final boolean recalculate_template = true;

    private final String vars = " -var=\"dataflow_classic_template_enabled=" + dataflow_classic_template_enabled + "\"" +
            " -var=\"dataflow_message_deduplication_enabled=" + generateMessageDuplicates + "\"" +
            " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + true + "\""+ // timestamp_attribute_enabled requires granting service account roles/pubsub.editor at project level
//                " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + !isMessageEventTimeIncreasing + "\""+
            " -var=\"skip_wait_on_job_termination=" + skip_wait_on_job_termination + "\"" +
            " -var=\"recalculate_template=" + recalculate_template + "\"";

    @Before
    public void cleanUp() throws IOException, InterruptedException {
        Process process = runBashProcess("terraform init && terraform destroy -auto-approve " + vars);
        logProcess(process);
        int statusCode = process.waitFor();
        Assert.assertEquals("init and destroy process should exit terraform with 0 statusCode code", 0, statusCode);
    }

    @After
    public void cleanUpAfter() throws IOException, InterruptedException {
        Process process = runBashProcess("terraform init && terraform destroy -auto-approve -target=module.dataflow_classic_template_job " + vars);
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
//        int numMessages = 23 * 60 * 10;
        int numMessages = 23 * 60;


        // when
        Process terraformApplyProcess = runBashProcess("terraform apply -auto-approve " + vars);

        logProcess(terraformApplyProcess);
        int status = terraformApplyProcess.waitFor();
        Assert.assertEquals("Should exit terraform with 0 status code", 0, status);

        LOGGER.info("Waiting 1 min for infra to start");
        Thread.sleep(60 * 1000);

        long firstEventTimeMillis = System.currentTimeMillis();
        Function<Integer, Long> fn = msgIdx -> isMessageEventTimeIncreasing ? (msgIdx == 1 ? firstEventTimeMillis : System.currentTimeMillis()) : firstEventTimeMillis;

        List<String> messageIds = sendNPubsubMessagesWithDelay(topic, numMessages, Duration.ofMillis(250), fn);

        // then
        int expectedMessageCount = generateMessageDuplicates ? numMessages / 2 : numMessages;
        List<String> avroRecordsAsStrings = waitUpTo5MinsForDataflowJobToWriteAvroFileToGCSBucket(gcsBucket, expectedMessageCount);

        LOGGER.info("Content of avro file(s) read from GCS: {}", avroRecordsAsStrings);

        LOGGER.info("Read {} elements", avroRecordsAsStrings.size());

        Assert.assertEquals(expectedMessageCount , avroRecordsAsStrings.size());

        int messageIndex = 1;
//        int messageIndex = numMessages;
        String messageId = messageIds.get(messageIndex - 1);
        String regex = "\\{\"" + BODY_WITH_ATTRIBUTES_AND_MESSAGE_ID + "\": " + "\"" + "body=" + MY_MSG_BODY + ":" + messageIndex + ", attributes=\\{" + EVENT_TIME_ATTRIBUTE + "=" + Instant.ofEpochMilli(firstEventTimeMillis) + ", " + MY_MSG_ATTR_NAME + "=" + MY_MSG_ATTR_VALUE + messageIndex + ", " + PUBLISH_TIME_ATTRIBUTE + "=" + "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z" + "}" + ", messageId=" + messageId
                + ", inputDataFreshnessMs=" + "\\d+" + ", customInputDataFreshnessMs=" + "\\d+" + ", customEventTimeInputDataFreshnessMs=" + "\\d+" + "\"}";

        LOGGER.info("regex={}", regex);
        Pattern pattern = Pattern.compile(regex);

        avroRecordsAsStrings.stream().filter(s -> s.contains("myMsgBody:" + messageIndex + ",")).forEach(s -> LOGGER.info("s={}", s));

        Assert.assertTrue(avroRecordsAsStrings.stream().anyMatch(s -> pattern.matcher(s).matches()));


//        String query = "select * from " + get("GCP_PROJECT") + "." + get("GCP_OWNER") + "_" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase() + ".my_table";
//        long actualMessageCount = waitUpTo10MinsForDataflowJobToPopulateBigQuery(query, expectedMessageCount);
//        Assert.assertEquals("Expected to get " + expectedMessageCount + " records from BigQuery", expectedMessageCount, actualMessageCount);

        int waitMinsBeforeDestroy = 1;
        LOGGER.info("Assertions passed, waiting {} min(s) before deleting resources", waitMinsBeforeDestroy);
        Thread.sleep(waitMinsBeforeDestroy * 60 * 1000);
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

    private List<String> sendNPubsubMessagesWithDelay(String topic, int numMessages, Duration sleepDuration, Function<Integer, Long> msgIdxToTimestampMillisFn) throws InterruptedException {
        List<String> messageIds = new ArrayList<>();

        AtomicInteger counter = new AtomicInteger(1);
        CountDownLatch countDownLatch = new CountDownLatch(numMessages);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

        ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(() -> {
            Publisher publisher = null;
            List<ApiFuture<String>> messageIdFutures = new ArrayList<>();


            int i = counter.get();
            if (i > numMessages) return;

            long start = 0;
            try {
                // Batch settings control how the publisher batches messages
                long requestBytesThreshold = 5000L; // default : 1000 bytes
                long messageCountBatchSize = 10L; // default : 100 message

                org.threeten.bp.Duration publishDelayThreshold = org.threeten.bp.Duration.ofMillis(100); // default : 1 ms

                // Publish request get triggered based on request size, messages count & time since last
                // publish, whichever condition is met first.
                BatchingSettings batchingSettings =
                        BatchingSettings.newBuilder()
                                .setElementCountThreshold(messageCountBatchSize)
                                .setRequestByteThreshold(requestBytesThreshold)
                                .setDelayThreshold(publishDelayThreshold)
                                .build();

                // Create a publisher instance with default settings bound to the topic
                publisher = Publisher.newBuilder(TopicName.of(get("GCP_PROJECT"), topic)).setBatchingSettings(batchingSettings).build();

                // schedule publishing one message at a time : messages get automatically batched
                for (int j = 0; j < 5; j++) {
                    countDownLatch.countDown();
                    int ii = counter.getAndIncrement();
                    if (ii > numMessages) return;

                    // values of myMsgAttrName attribute are not unique but duplicated: 0, 2, 2, 4, 4 ...
                    // attrs: M1: myMsgAttrName1=myMsgAttrValue0, M2: myMsgAttrName2=myMsgAttrValue=2, M3: myMsgAttrName3=myMsgAttrValue=2, M4: myMsgAttrName4=myMsgAttrValue=4
                    String myMsgAttrValue = generateMessageDuplicates ? MY_MSG_ATTR_VALUE + (ii + 1 - ((ii + 1) % 2)) : MY_MSG_ATTR_VALUE + ii;

                    long eventTimeMillis = msgIdxToTimestampMillisFn.apply(ii);

                    // values of myMsgAttrName attr
                    if (j == 0) {
                        start = System.currentTimeMillis();
                        LOGGER.info("Sending PS msg ({} of {}), {}, et={}", ii, numMessages, myMsgAttrValue, Instant.ofEpochMilli(eventTimeMillis).toDateTime());
                    }

                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(ByteString.copyFromUtf8(MY_MSG_BODY + ":" + ii))
                        .putAllAttributes(ImmutableMap.of(
                                MY_MSG_ATTR_NAME, myMsgAttrValue,
                                PUBLISH_TIME_ATTRIBUTE, Instant.now().toString(),
                                EVENT_TIME_ATTRIBUTE, Instant.ofEpochMilli(eventTimeMillis).toString()
                        ))
                        .build();

                    ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                    messageIdFutures.add(messageIdFuture);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                // Wait on any pending publish requests.
                try {
                    List<String> batchMessageIds = ApiFutures.allAsList(messageIdFutures).get();
                    messageIds.addAll(batchMessageIds);

                    LOGGER.info("Published batched " + batchMessageIds.size() + " messages in " + (System.currentTimeMillis() - start) + " millis");
                    if (publisher != null) {
                        // When finished with the publisher, shutdown to free up resources.
                        publisher.shutdown();
                        publisher.awaitTermination(1, TimeUnit.MINUTES);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            //LOGGER.info("Sent PS msg ({} of {}), {}, et={}", i, numMessages, myMsgAttrValue, Instant.ofEpochMilli(eventTimeMillis).toDateTime());
        }, 10, sleepDuration.toMillis(), TimeUnit.MILLISECONDS);

        countDownLatch.await();
        handle.cancel(false);

        /*
        for (int i = 1; i <= numMessages; i++) {
            long start = System.currentTimeMillis();
            // values of myMsgAttrName attribute are not unique but duplicated: 0, 2, 2, 4, 4 ...
            // attrs: M1: myMsgAttrName1=myMsgAttrValue0, M2: myMsgAttrName2=myMsgAttrValue=2, M3: myMsgAttrName3=myMsgAttrValue=2, M4: myMsgAttrName4=myMsgAttrValue=4
            String myMsgAttrValue = generateMessageDuplicates ? MY_MSG_ATTR_VALUE + (i + 1 - ((i + 1) % 2)) : MY_MSG_ATTR_VALUE + i;

            long eventTimeMillis = msgIdxToTimestampMillisFn.apply(i);
            String messageId = sendMessageToPubsub(MY_MSG_BODY + ":" + i, myMsgAttrValue, eventTimeMillis, topic);
            messageIds.add(messageId);
            long end = System.currentTimeMillis();
            long diff = end - start;
            long actualSleep = sleepDuration.toMillis() - diff < 0 ? 0 : sleepDuration.toMillis() - diff;
            LOGGER.info("Sent PS msg ({} of {}), {}, et={}, diff={}, sleep={}", i, numMessages, myMsgAttrValue, Instant.ofEpochMilli(eventTimeMillis).toDateTime(), diff, actualSleep);
            Thread.sleep(actualSleep);
//            if (i % (120) == 0) {
//                Thread.sleep(2 * 60 * 1000); // temporarily pause sending for 2 mins
//            }
        }
        */
//        String lastMessageId = sendMessageToPubsub(MY_MSG_BODY + numMessages, MY_MSG_ATTR_NAME + numMessages, MY_MSG_ATTR_VALUE + numMessages, lastEventTimeMillis, topic);
//        LOGGER.info("Sent pubsub message ({} of {}), messageId={}", numMessages, numMessages, lastMessageId);
//        messageIds.add(lastMessageId);
        return messageIds;
    }

    private String sendMessageToPubsub(String myMsgBody, String myMsgAttrValue, long eventTimeMillis, String topic) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(get("GCP_PROJECT"), topic)).build();
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(myMsgBody))
                .putAllAttributes(ImmutableMap.of(
                        MY_MSG_ATTR_NAME, myMsgAttrValue,
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
        int limit = 360;
        for (int i = 1; i <= limit; i++) {
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
                        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tempAvroFile, datumReader)) {
                            while (dataFileReader.hasNext()) {
                                GenericRecord genericRecord = dataFileReader.next();
                                results.add(genericRecord.toString());
                            }
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
                    LOGGER.info("Waiting for dataflow job to write read all messages to GCS ({}/{}) ...(attempt {}/{})", results.size(), expectedNumMessages, i, limit);
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
