package com.bawi.beam.dataflow;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PubSubPublisherSubscriberTest {
    String project = System.getenv("GCP_PROJECT");
    String topic = System.getenv("GCP_OWNER") + "-" + "topic";
    String subscription = topic + "-sub";

    @Before
    public void before() throws IOException {
        List<String> topics = listTopics(project);
        if (topics.stream().noneMatch(t -> t.endsWith(topic))) {
            createTopic(project, topic);
        }

        List<String> subscriptions = listSubscriptions(project);
        if (subscriptions.stream().noneMatch(s -> s.endsWith(subscription))) {
            createPullSubscription(project, topic, subscription);
        }
    }

    @After
    public void after() throws IOException {
        List<String> subscriptions = listSubscriptions(project);
        if (subscriptions.stream().anyMatch(s -> s.endsWith(subscription))) {
            deleteSubscription(project, subscription);
        }

        List<String> topics = listTopics(project);
        if (topics.stream().anyMatch(t -> t.endsWith(topic))) {
            deleteTopic(project, topic);
        }
    }


    @Test
    public void test() throws IOException, ExecutionException, InterruptedException {
        // given

        String text = "abc123";

        // when
        publish(project, topic, compress(text.getBytes()));
        List<byte[]> messages = subscribe(project, subscription);

        // then
        Assert.assertEquals(text, new String(decompress(messages.get(0))));
    }

    private static List<byte[]> subscribe(String project, String subscription) {
        List<byte[]> messages = new ArrayList<>();
        try {
            subscribe(project, subscription, messages::add);
            return messages;
        } catch (TimeoutException e) {
            return messages;
        }
    }

    private static void subscribe(String project, String subscription, Consumer<byte[]> c) throws TimeoutException {
        MessageReceiver receiver = (message, consumer) -> {
            ByteString data = message.getData();
            byte[] bytes = data.toByteArray();
            c.accept(bytes);
            System.out.println("received: message.getMessageId()=" + message.getMessageId());
            System.out.println("received: message.getData().size()=" + message.getData().size());
            consumer.ack();
        };

        ProjectSubscriptionName projectSubscription = ProjectSubscriptionName.of(project, subscription);
        Subscriber subscriber = Subscriber.newBuilder(projectSubscription, receiver).build();
        subscriber.startAsync().awaitRunning();
        subscriber.awaitTerminated(10, TimeUnit.SECONDS);
    }

    private static void publish(String project, String topic, byte[] bytes) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(project, topic)).build();
        ByteString byteString = ByteString.copyFrom(bytes);
        System.out.println("published: byteString.size()=" + byteString.size());
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(byteString)
                .build();

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        publisher.shutdown();
        publisher.awaitTermination(20, TimeUnit.SECONDS);
        System.out.println("published: messageId=" + messageId);
    }

    private static byte[] compress(byte[] bytes) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bytes.length);
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteStream)) {
            gzipOutputStream.write(bytes);
        }
        return byteStream.toByteArray();
    }

    static byte[] decompress(byte[] compressedPayload) throws IOException {
        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedPayload);
                GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream)
        ) {
            return IOUtils.toByteArray(gzipInputStream);
        }
    }

    public static List<String> listTopics(String projectId) throws IOException {
        List<String> found = new ArrayList<>();
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            for (Topic topic : topicAdminClient.listTopics(projectName).iterateAll()) {
                found.add(topic.getName());
            }
        }
        return found;
    }

    public static void createTopic(String projectId, String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            Topic topic = topicAdminClient.createTopic(topicName);
            System.out.println("Created topic: " + topic.getName());
        }
    }

    private static void createPullSubscription(
            String projectId, String topicId, String subscriptionId) throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);
            // Create a pull subscription with default acknowledgement deadline of 10 seconds.
            // Messages not successfully acknowledged within 10 seconds will get resent by the server.
            Subscription subscription =
                    subscriptionAdminClient.createSubscription(
                            subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
            System.out.println("Created pull subscription: " + subscription.getName());
        }
    }

    private static List<String> listSubscriptions(String projectId) throws IOException {
        List<String> found = new ArrayList<>();
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            for (Subscription subscription : subscriptionAdminClient.listSubscriptions(projectName).iterateAll()) {
                found.add(subscription.getName());
            }
        }
        return found;
    }

    private static void deleteTopic(String projectId, String topicId) throws IOException {
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            TopicName topicName = TopicName.of(projectId, topicId);
            try {
                topicAdminClient.deleteTopic(topicName);
                System.out.println("Deleted topic: " + topicName);
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    static void deleteSubscription(String projectId, String subscriptionId)
            throws IOException {
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);
            try {
                subscriptionAdminClient.deleteSubscription(subscriptionName);
                System.out.println("Deleted subscription: " + subscriptionName);
            } catch (NotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
