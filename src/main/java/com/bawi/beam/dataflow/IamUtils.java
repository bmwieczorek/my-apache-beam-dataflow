package com.bawi.beam.dataflow;


import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.pubsub.v1.ProjectTopicName;

public class IamUtils {


    public static void addPubSubAdminRoleToServiceAccount(
            String projectId, String topicId, String serviceAccountEmail, String role) throws Exception {

        String serviceAccountMember = "serviceAccount:" + serviceAccountEmail;

        // Grant role on topic
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
            GetIamPolicyRequest getIamPolicyRequest = GetIamPolicyRequest.newBuilder().setResource(topicName.toString()).build();
            Policy policy = topicAdminClient.getIamPolicy(getIamPolicyRequest);

            Policy updatedPolicy = policy.toBuilder()
                    .addBindings(Binding.newBuilder()
                            .setRole(role)
                            .addMembers(serviceAccountMember)
                            .build())
                    .build();

            SetIamPolicyRequest setIamPolicyRequest = SetIamPolicyRequest.newBuilder()
                    .setResource(topicName.toString())
                    .setPolicy(updatedPolicy)
                    .build();

            topicAdminClient.setIamPolicy(setIamPolicyRequest);
        }
    }
}

