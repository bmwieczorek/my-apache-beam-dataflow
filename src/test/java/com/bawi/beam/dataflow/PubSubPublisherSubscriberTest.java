package com.bawi.beam.dataflow;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.bawi.beam.dataflow.PubSubUtils.createTopicAndSubscription;
import static com.bawi.beam.dataflow.PubSubUtils.deleteSubscriptionAndTopic;
import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;

@SuppressWarnings("SameParameterValue")
public class PubSubPublisherSubscriberTest {

    @Test
    public void shouldPublishAndReceiveGzipCompressedMessages() throws Exception {
        // given
        String text1 = "abc123";
        String text2 = "cdef4567";

        // when
        publish(PROJECT, TOPIC_NAME, gzip(text1));
        publish(PROJECT, TOPIC_NAME, gzip(text2));

        List<byte[]> receivedMessages;
        try (MySubscriber mySubscriber = new MySubscriber(PROJECT, SUBSCRIPTION_NAME)) {
            receivedMessages = pollExpectedNMessagesWithRetry(mySubscriber, 2, 10);
        }
        List<String> receivedDecompressedMessage = receivedMessages.stream().map(bytes -> new String(gunzip(bytes))).toList();

        // then
        Assert.assertEquals(2, receivedMessages.size());
        Assert.assertTrue(receivedDecompressedMessage.contains(text1));
        Assert.assertTrue(receivedDecompressedMessage.contains(text2));
    }

    private static List<byte[]> pollExpectedNMessagesWithRetry(MySubscriber mySubscriber, int expectedCount, int maxRetries) throws InterruptedException {
        List<byte[]> messages = new ArrayList<>();
        for (int retryCounter = 0; retryCounter < maxRetries; retryCounter++) {
            messages = mySubscriber.getMessages();
            if (messages.size() < expectedCount) {
                LOGGER.info("Waiting for messages to be received...");
                TimeUnit.SECONDS.sleep(1);
            } else {
                break;
            }
        }
        return messages;
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(PubSubPublisherSubscriberTest.class);

    static class MySubscriber implements AutoCloseable {
        private final List<byte[]> messages = new CopyOnWriteArrayList<>();

        private final String project;
        private final String subscription;
        private final Subscriber subscriber;

        public MySubscriber(String project, String subscription) {
            this.project = project;
            this.subscription = subscription;
            MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer ackReplyConsumer) -> {
                ByteString data = message.getData();
                byte[] bytes = data.toByteArray();
                messages.add(bytes);
                LOGGER.info("Received: message.getMessageId()={}, message.getData().size()={}", message.getMessageId(), message.getData().size());
                ackReplyConsumer.ack();
            };

            ProjectSubscriptionName projectSubscription = ProjectSubscriptionName.of(project, subscription);
            subscriber = Subscriber.newBuilder(projectSubscription, receiver).build();
            LOGGER.info("Starting subscribing to project: {}, subscription: {}", project, subscription);
            subscriber.startAsync().awaitRunning();
        }

        @Override
        public void close() throws TimeoutException {
            LOGGER.info("Stopping subscribing to project: {}, subscription: {}", project, subscription);
            subscriber.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
            LOGGER.info("Stopped subscribing to project: {}, subscription: {}", project, subscription);
        }

        public List<byte[]> getMessages() {
            return new ArrayList<>(messages);
        }
    }

    private static void publish(String project, String topic, byte[] bytes) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(project, topic)).build();
        try {
            ByteString byteString = ByteString.copyFrom(bytes);
            LOGGER.info("Publishing: data with byteString size={}", byteString.size());
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(byteString).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            LOGGER.info("Published: messageId={}", messageId);
        } finally {
            publisher.shutdown();
            publisher.awaitTermination(20, TimeUnit.SECONDS);
        }
    }


    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String TOPIC_NAME = System.getenv("GCP_OWNER") + "-" + "topic";
    private static final String SUBSCRIPTION_NAME = TOPIC_NAME + "-sub";

    @Before
    public void before() throws IOException {
        createTopicAndSubscription(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }

    @After
    public void after() throws IOException {
        deleteSubscriptionAndTopic(PROJECT, TOPIC_NAME, SUBSCRIPTION_NAME);
    }
}
