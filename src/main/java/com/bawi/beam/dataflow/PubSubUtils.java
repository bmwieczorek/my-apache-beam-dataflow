package com.bawi.beam.dataflow;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PubSubUtils {

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

    public static void createPullSubscription(
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

    public static List<String> listSubscriptions(String projectId) throws IOException {
        List<String> found = new ArrayList<>();
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            ProjectName projectName = ProjectName.of(projectId);
            for (Subscription subscription : subscriptionAdminClient.listSubscriptions(projectName).iterateAll()) {
                found.add(subscription.getName());
            }
        }
        return found;
    }

    public static void deleteTopic(String projectId, String topicId) throws IOException {
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

    public static void deleteSubscription(String projectId, String subscriptionId)
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

    public static void createTopicAndSubscription(String project, String topic, String subscription) throws IOException {
        List<String> topics = listTopics(project);
        if (topics.stream().noneMatch(t -> t.endsWith(topic))) {
            createTopic(project, topic);
        }

        List<String> subscriptions = listSubscriptions(project);
        if (subscriptions.stream().noneMatch(s -> s.endsWith(subscription))) {
            createPullSubscription(project, topic, subscription);
        }
    }

    public static void deleteSubscriptionAndTopic(String project, String topic, String subscription) throws IOException {
        List<String> subscriptions = listSubscriptions(project);
        if (subscriptions.stream().anyMatch(s -> s.endsWith(subscription))) {
            deleteSubscription(project, subscription);
        }

        List<String> topics = listTopics(project);
        if (topics.stream().anyMatch(t -> t.endsWith(topic))) {
            deleteTopic(project, topic);
        }
    }
}
