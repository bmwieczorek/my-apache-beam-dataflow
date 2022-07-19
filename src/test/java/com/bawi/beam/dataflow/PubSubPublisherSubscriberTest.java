package com.bawi.beam.dataflow;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Assert;
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

    @Test
    public void test () throws IOException, ExecutionException, InterruptedException {
        // given
        String topic = System.getenv("GCP_OWNER") + "-" + "topic";
        String text = "abc123";

        // when
        publish(topic, compress(text.getBytes()));
        List<byte[]> messages = subscribe(topic + "-sub");

        // then
        Assert.assertEquals(text, new String(decompress(messages.get(0))));
    }

    private static List<byte[]> subscribe(String subscription) {
        List<byte[]> messages = new ArrayList<>();
        try {
            subscribe(subscription, messages::add);
            return messages;
        } catch (TimeoutException e) {
            return messages;
        }
    }

    private static void subscribe(String subscription, Consumer<byte[]> c) throws TimeoutException {
        MessageReceiver receiver = (message, consumer) -> {
            ByteString data = message.getData();
            byte[] bytes = data.toByteArray();
            c.accept(bytes);
            System.out.println("received: message.getMessageId()=" + message.getMessageId());
            System.out.println("received: message.getData().size()=" + message.getData().size());
            consumer.ack();
        };

        ProjectSubscriptionName projectSubscription = ProjectSubscriptionName.of(System.getenv("GCP_PROJECT"), subscription);
        Subscriber subscriber = Subscriber.newBuilder(projectSubscription, receiver).build();
        subscriber.startAsync().awaitRunning();
        subscriber.awaitTerminated(10, TimeUnit.SECONDS);
    }

    private static void publish(String topic, byte[] bytes) throws IOException, InterruptedException, ExecutionException {
        Publisher publisher = Publisher.newBuilder(TopicName.of(System.getenv("GCP_PROJECT"), topic)).build();
        ByteString byteString = ByteString.copyFrom(bytes);
        System.out.println("published: byteString.size()=" + byteString.size());
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(byteString)
                .build();

        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        String messageId = messageIdFuture.get();
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
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
}
