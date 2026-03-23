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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
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
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static com.bawi.beam.dataflow.MyPubsubToGCSJob.*;

public class MyPubsubToGCSJobIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyPubsubToGCSJobIntegrationTest.class);
    private static final String MSG_BODY = "msgBody";
    private static final String MSG_ATTR_VALUE = "msgAttrValue";
    private final boolean dataflow_classic_template_enabled = true;
    private final boolean generateMessageDuplicates = true;
    private final boolean dataflowDeduplicationEnabled = true;
    private final boolean isMessageEventTimeIncreasing = true;
    private final boolean skip_wait_on_job_termination = false;
    private final boolean recalculate_template = true;

    private final String vars = " -var=\"dataflow_classic_template_enabled=" + dataflow_classic_template_enabled + "\"" +
            " -var=\"dataflow_message_deduplication_enabled=" + dataflowDeduplicationEnabled + "\"" +
            " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + true + "\"" + // timestamp_attribute_enabled requires granting service account roles/pubsub.editor at project level
            " -var=\"dataflow_custom_event_time_timestamp_attribute=" + EVENT_TIME_ATTRIBUTE + "\"" +
//                " -var=\"dataflow_custom_event_time_timestamp_attribute_enabled=" + !isMessageEventTimeIncreasing + "\""+
            " -var=\"skip_wait_on_job_termination=" + skip_wait_on_job_termination + "\"" +
            " -var=\"recalculate_template=" + recalculate_template + "\"";

    @Before
    public void prepareBefore() throws IOException, InterruptedException {
        assertEnvVariablesPresent();
        rebuildUberJar(); // jar needs to be present beforehand as it referenced in later by terraform
        runTerraformDestroy();
    }

    @After
    public void cleanUpAfter() throws IOException, InterruptedException {
        LOGGER.info("Running cleanUpAfter");
        // Process process = runBashProcess("terraform init && terraform destroy -auto-approve -target=module.dataflow_classic_template_job " + vars);
        runTerraformDestroy();
    }

    @Test
    public void testE2E() throws IOException, InterruptedException {
        // given
        String topic = get("GCP_OWNER") + "-topic";
        String bucket = get("GCP_PROJECT") + "-" + get("GCP_OWNER") + "-" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase();
        LOGGER.info("topic={}, bucket={}", topic, bucket);

        // when
        Process terraformApplyProcess = runBashProcess("terraform apply -auto-approve " + vars);
        logProcess(terraformApplyProcess);
        int status = terraformApplyProcess.waitFor();
        Assert.assertEquals("Should exit terraform apply with 0 status code", 0, status);

        LOGGER.info("Waiting 1 min for infra to start");
        Thread.sleep(60 * 1000);

        // send messages to PubSub with fixed delay
        int numMessagesToSend = 2 * 4 * 10 * 60; // 1380 (2*4*10*60=4800 if for 2 minutes)
        long firstEventTimeMillis = System.currentTimeMillis();
        Function<Integer, Long> fn = msgIdx -> isMessageEventTimeIncreasing ? (msgIdx == 1 ? firstEventTimeMillis : System.currentTimeMillis()) : firstEventTimeMillis;
        List<String> messageIds = sendNPubsubMessagesWithDelay(topic, numMessagesToSend, fn);

        @SuppressWarnings({"ConstantValue", "unused"})
        int expectedMessageCount = generateMessageDuplicates && dataflowDeduplicationEnabled ? numMessagesToSend / 2 : numMessagesToSend;

        // then
        LOGGER.info("Sent {} messages, got {} messageIds" , numMessagesToSend, messageIds.size());
        Assert.assertEquals("Sent " + numMessagesToSend + " messages, got " + messageIds.size() + " messageIds", numMessagesToSend, messageIds.size());

        int numberOfReadRetries = 240; // 300 retries with 5 sec deply gives 25 mins of checking for generic records in job's avro output

        String objectPathPrefix = "output-windowedWrites/";
        List<String> outputWindowedAvroRecords = readAvroFileGeneratedByDataflowJob(bucket, objectPathPrefix, expectedMessageCount, numberOfReadRetries);
        LOGGER.info("Sent {} messages, read {} avro records from {}", expectedMessageCount, outputWindowedAvroRecords.size(), objectPathPrefix);
        Assert.assertEquals(expectedMessageCount, outputWindowedAvroRecords.size());
        LOGGER.info("Content of first avro file read from from {}: {}", objectPathPrefix, outputWindowedAvroRecords.getFirst());
        LOGGER.info("Content of last avro file read from from {}: {}", objectPathPrefix, outputWindowedAvroRecords.getLast());


        objectPathPrefix = "output/";
        List<String> outputAvroRecords = readAvroFileGeneratedByDataflowJob(bucket, objectPathPrefix, expectedMessageCount, numberOfReadRetries);
        LOGGER.info("Sent {} messages, read {} avro records from {}", expectedMessageCount, outputAvroRecords.size(), objectPathPrefix);
        Assert.assertEquals(expectedMessageCount, outputAvroRecords.size());
        LOGGER.info("Content of first avro file read from from {}: {}", objectPathPrefix, outputAvroRecords.getFirst());
        LOGGER.info("Content of last avro file read from from {}: {}", objectPathPrefix, outputAvroRecords.getLast());

        List<String> filteredMessages = outputAvroRecords.stream()
                .filter(s -> s.contains(MSG_BODY + ":1,") || s.contains(MSG_BODY + ":2,")).toList();
        filteredMessages.forEach(m -> LOGGER.info("Read message with body ending with :1 or :2: {}", m));
        @SuppressWarnings({"ConstantValue", "unused"})
        int expectedCount = generateMessageDuplicates && dataflowDeduplicationEnabled ? 1 : 2;
        LOGGER.info("Expected filtered {} message(s) from GCS, got {}", expectedCount, filteredMessages.size());
        Assert.assertEquals("Expected filtered " + expectedCount + " messages from GCS, got " + filteredMessages.size(), expectedCount, filteredMessages.size());


        long actualBQRecordsCount = readWithRetriesBQTableRowsPopulatedByDataflowJob(expectedMessageCount);
        LOGGER.info("Expected {} rows read from BQ, got {}", expectedMessageCount, actualBQRecordsCount);
        Assert.assertEquals("Expected " + expectedMessageCount + " row read from BQ, got " + actualBQRecordsCount, expectedMessageCount, actualBQRecordsCount);

        int waitMinsBeforeDestroy = 1;
        LOGGER.info("Assertions passed, waiting {} min(s) before deleting resources", waitMinsBeforeDestroy);
        Thread.sleep(waitMinsBeforeDestroy * 60 * 1000);
    }

    private void runTerraformDestroy() throws IOException, InterruptedException {
        Process process = runBashProcess("terraform init && terraform destroy -auto-approve " + vars);
        logProcess(process);
        int statusCode = process.waitFor();
        Assert.assertEquals("init and destroy process should exit terraform with 0 statusCode code", 0, statusCode);
    }

    private String get(String variable) {
        return System.getProperty(variable, System.getenv(variable));
    }

    private List<String> sendNPubsubMessagesWithDelay(String topic, int numMessagesToSend, Function<Integer, Long> msgIdxToTimestampMillisFn) throws InterruptedException {
        int logMessagesInterval = 10;
        int messagesBatchSize = 10;
//        Duration periodMillisBetweenScheduledThreads = Duration.ofMillis(60 * 1000); // send batch every 1m
        Duration periodMillisBetweenScheduledThreads = Duration.ofMillis(250); // send batch every 250 ms

        int totalBatches = (numMessagesToSend + messagesBatchSize - 1) / messagesBatchSize;
        List<String> messageIds = new CopyOnWriteArrayList<>();
        AtomicInteger counter = new AtomicInteger(1);
        CountDownLatch batchCompletionLatch = new CountDownLatch(totalBatches);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
        ScheduledFuture<?> handle = scheduler.scheduleAtFixedRate(() -> {
            Publisher publisher = null;
            List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

            int i = counter.get();
            if (i > numMessagesToSend) return;

            long timeMillisStart = System.currentTimeMillis();
            try {
                // Batch settings control how the publisher batches messages
                long requestBytesThreshold = 5000L; // default : 1000 bytes
                long messageCountBatchSize = 10L; // default : 100 message

                org.threeten.bp.Duration publishDelayThreshold = org.threeten.bp.Duration.ofMillis(300); // default : 1 ms

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
                int batchSize = 0;
                for (int j = 1; j <= messagesBatchSize; j++) {
                    int msgCounter = counter.getAndIncrement();
                    if (msgCounter > numMessagesToSend) return;

                    // values of myMsgAttrName attribute are not unique but duplicated: 0, 2, 2, 4, 4 ...
                    // attrs: M1: myMsgAttrName1=msgAttrValue0, M2: myMsgAttrName2=msgAttrValue=2, M3: myMsgAttrName3=msgAttrValue=2, M4: myMsgAttrName4=msgAttrValue=4
//                    String msgAttrValue = generateMessageDuplicates ? MSG_ATTR_VALUE + (msgCounter + 1 - ((msgCounter + 1) % 2)) : MSG_ATTR_VALUE + msgCounter;
                    String msgAttrValue = MSG_ATTR_VALUE + getFirstMessageIndex(generateMessageDuplicates, msgCounter);
                    long eventTimeMillis = msgIdxToTimestampMillisFn.apply(msgCounter);
                    String body = MSG_BODY + ":" + msgCounter;

                    // initial logging of first messages
                    if (msgCounter <= logMessagesInterval) {
                        LOGGER.info("Sending message {} of {} with body {} and attrs {}={}, et={}",
                                msgCounter, numMessagesToSend, body, MSG_ATTR_NAME, msgAttrValue, Instant.ofEpochMilli(eventTimeMillis).toDateTime());
                    }

                    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(body))
                            .putAllAttributes(ImmutableMap.of(
                                    MSG_ATTR_NAME, msgAttrValue,
                                    PUBLISH_TIME_ATTRIBUTE, Instant.now().toString(),
                                    EVENT_TIME_ATTRIBUTE, Instant.ofEpochMilli(eventTimeMillis).toString()
                            ))
                            .build();

                    ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                    messageIdFutures.add(messageIdFuture);
                    batchSize = j;
                }
                List<String> batchMessageIds = ApiFutures.allAsList(messageIdFutures).get();
                messageIds.addAll(batchMessageIds);
                int msgCounter = counter.get() - 1;
                if (msgCounter % logMessagesInterval == 0 || (counter.get() - 1) == numMessagesToSend) {
                    LOGGER.info("Sent message {} of {} in {}-element(s) batch in {}ms", msgCounter, numMessagesToSend, batchSize, System.currentTimeMillis() - timeMillisStart);
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } finally {
                batchCompletionLatch.countDown();
                try {
                    if (publisher != null) {
                        // When finished with the publisher, shutdown to free up resources.
                        publisher.shutdown();
                        publisher.awaitTermination(1, TimeUnit.MINUTES);
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("Publisher interrupted", e);
                }
            }
        }, 10, periodMillisBetweenScheduledThreads.toMillis(), TimeUnit.MILLISECONDS);

        batchCompletionLatch.await();
        handle.cancel(false);
        scheduler.shutdown();
        if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
            LOGGER.warn("Scheduler did not terminate within 1 minute");
        }

        return new ArrayList<>(messageIds);
    }

    private static int getFirstMessageIndex(@SuppressWarnings("SameParameterValue") boolean generateMessageDuplicates, int i) {
        return generateMessageDuplicates ? (i + 1 - ((i + 1) % 2)) : i;
    }

    private List<String> readAvroFileGeneratedByDataflowJob(String bucketName, @SuppressWarnings("SameParameterValue") String objectPathPrefix, int expectedNumMessages, int limitRetries) throws InterruptedException {
        int retryDelaySecs = 5;

        List<String> outputAvroRecordsAsStrings = new ArrayList<>();
        Storage storage = StorageOptions.newBuilder().setProjectId(get("GCP_PROJECT")).build().getService();
        for (int i = 1; i <= limitRetries; i++) {
            Page<Blob> blobs = storage.list(bucketName);
            List<Blob> filteredBlobs = StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                    .filter(blob -> blob.getName().startsWith(objectPathPrefix))
                    .filter(blob -> blob.getName().endsWith(".avro"))
                    // .peek(b -> LOGGER.info("filtered blob: {}, {}", b, b.getName()))
                    .toList();

            LOGGER.info("Found {} *.avro in {}", filteredBlobs.size(), objectPathPrefix);

            if (!filteredBlobs.isEmpty()) {
                List<String> results = new ArrayList<>();
                filteredBlobs.forEach(b -> {
                    try {
                        File tempAvroFile = File.createTempFile("avro-", ".avro");

                        int maxRetries = 5;
                        for (int idx = 1; idx <= maxRetries; idx++) {
                            try {
                                b.downloadTo(Paths.get(tempAvroFile.toURI()));
                                break;
                            } catch (StorageException e) {
                                LOGGER.warn("Failed to download avro file {}. Retrying in {} secs, attempt {} of {}", tempAvroFile.getAbsolutePath(), idx, idx, maxRetries, e);
                                try {
                                    Thread.sleep(idx * 1000L);
                                } catch (InterruptedException ex) {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }

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
                        LOGGER.error("Problem downloading object from GCS: {}", b, e);
                    }
                });

                if (results.size() == expectedNumMessages) {
                    LOGGER.info("Filtered {} avro paths", filteredBlobs.size());
                    LOGGER.info("Filtered first avro path: {}", filteredBlobs.getFirst().getName());
                    LOGGER.info("Filtered last avro path: {}", filteredBlobs.getLast().getName());
                    return results;
                } else {
                    LOGGER.info("Read {}/{} avro records, retry {}/{}", results.size(), expectedNumMessages, i, limitRetries);
                    Thread.sleep(retryDelaySecs * 1000L);
                    outputAvroRecordsAsStrings = results;
                }
            } else {
                LOGGER.info("Waiting for Dataflow to write, retry {}/{}", i, limitRetries);
                Thread.sleep(retryDelaySecs * 1000L);
            }
        }
        return outputAvroRecordsAsStrings;
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
        processBuilder.command("bash", "-c", "test -f ~/.profile && source ~/.profile || true && " + cmd);
        return processBuilder.start();
    }

    private Process runBashProcess(String cmd) throws IOException {
        // Process process = Runtime.getRuntime().exec("./run-terraform.sh", null, new File("terraform/" + MyPubsubToGCSJob.class.getSimpleName()));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File("terraform/" + MyPubsubToGCSJob.class.getSimpleName()));
        processBuilder.command("bash", "-c", "test -f ~/.profile && source ~/.profile || true && " + cmd);
        return processBuilder.start();
    }

    private void rebuildUberJar() throws IOException, InterruptedException {
        Process mvnProcess =
                dataflow_classic_template_enabled ?
                        runMvnAsBashProcess("mvn clean package -Pdist -DskipTests")
                        :
                        runMvnAsBashProcess("mvn clean package -Pbuild-and-deploy-flex-template -Dgcp.project.id=${GCP_PROJECT} -DskipTests");
        logProcess(mvnProcess);
        int mvnProcessStatus = mvnProcess.waitFor();
        Assert.assertEquals("mvn build should exit with 0 status code", 0, mvnProcessStatus);
    }

    private void assertEnvVariablesPresent() throws IOException {
        runMvnAsBashProcess("env");
        Assert.assertNotNull("Expected Google Project ID to be set as env variable", get("GCP_PROJECT"));
        Assert.assertNotNull("Expected Google Project resource owner to be set as env variable", get("GCP_OWNER"));
        LOGGER.info("Read env variables: GCP_PROJECT={}, GCP_OWNER={}", get("GCP_PROJECT"), get("GCP_OWNER"));
    }

    private long readWithRetriesBQTableRowsPopulatedByDataflowJob(int expectedRowsCount) throws InterruptedException {
        String query = "select * from " + get("GCP_PROJECT") + "." + get("GCP_OWNER") + "_" + MyPubsubToGCSJob.class.getSimpleName().toLowerCase() + ".my_table";
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
   .apply(PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(options.getSubscription()).withIdAttribute(MSG_ATTR_NAME).withTimestampAttribute(ConcatBodyAttrAndMsgIdFn.EVENT_TIME_ATTRIBUTE))

Test
    private final boolean isMessageAttributeValueUnique = false;
    private final boolean isMessageEventTimeIncreasing = false;

2022-09-18 19:27:22,413 INFO  Sent pubsub message (1 of 600), msgAttrValue2, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:23,123 INFO  Sent pubsub message (2 of 600), msgAttrValue2, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:24,222 INFO  Sent pubsub message (3 of 600), msgAttrValue4, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:27:24,944 INFO  Sent pubsub message (4 of 600), msgAttrValue4, eventTime=2022-09-18T19:27:19.447+02:00
...
2022-09-18 19:41:58,484 INFO  Sent pubsub message (597 of 600), msgAttrValue598, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:41:59,143 INFO  Sent pubsub message (598 of 600), msgAttrValue598, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:41:59,797 INFO  Sent pubsub message (599 of 600), msgAttrValue600, eventTime=2022-09-18T19:27:19.447+02:00
2022-09-18 19:42:00,476 INFO  Sent pubsub message (600 of 600), msgAttrValue600, eventTime=2022-09-18T19:27:19.447+02:00
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
...
AfterPubsub_threadId_Thread-73:812  11


177 threads with worker harness 64

 */
